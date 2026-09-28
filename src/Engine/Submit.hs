{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Engine.Submit
  ( Context(..)
  , SubmitConfig(..)
  , SubmitOk(..)
  , SubmitError(..)
  , SubmitHandle(..)
  , startSubmitEngine
  , SubmitMsg(..)
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, forM_)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae
import Data.Int (Int32)
import qualified Data.Map as Mp
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)
import qualified Data.Vector as V

import GHC.Generics (Generic)

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx

import qualified DB.EngineStmt as Es
import qualified Engine.Runner as R
import qualified Engine.Support as Sup
import qualified Service.Types as St

--------------------------------------------------------------------------------
-- Public API

data Context = Context
  { pgPoolCT      :: Pool.Pool
  , nodeIdCT      :: Text
  , sendRequestCT :: Vector St.BatchRequest -> IO (Either SubmitError SubmitOk)
  , enqueuePollCT :: UUID ->Text -> IO () -- fast-path: batch uid to Engine.Poll
  }

data SubmitConfig = SubmitConfig
  { pollIntervalMicrosSC :: Int
  , batchSizeSC          :: Int
  , queueDepthSC         :: Int
  , workerCountSC        :: Int
  , claimTtlSecondsSC    :: Int32
  }
  deriving (Show, Eq, Generic)

-- Provider-side success:
-- - providerBatchIdSO: provider's batch id (text)
-- - batchUidSO: our batch uid (uuid) used throughout DB + polling + fetching
data SubmitOk = SubmitOk
  { providerBatchIdSO :: Text
  , batchUidSO        :: UUID
  }
  deriving (Show, Eq, Generic)

data SubmitError = SubmitError
  { codeSE    :: Text
  , messageSE :: Text
  }
  deriving (Show, Eq, Generic)

newtype SubmitEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

data SubmitHandle = SubmitHandle
  { asyncSH   :: Async ()
  , enqueueSH :: SubmitMsg -> IO ()
  }

data SubmitMsg
  = SubmitKick
  deriving (Show, Eq, Generic)

--------------------------------------------------------------------------------
-- Internal job model

data ClaimedRequest = ClaimedRequest
  { requestIdCR   :: UUID
  , requestTextCR :: Text
  }
  deriving (Show, Eq, Generic)

--------------------------------------------------------------------------------
-- Engine lifecycle

startSubmitEngine :: Context -> SubmitConfig -> IO SubmitHandle
startSubmitEngine ctxt cfg = do
  h <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthSC
    , workerCountES = cfg.workerCountSC
    , feedersES     = [kickFeeder cfg]
    , workerES      = \_ msg -> submitWorker ctxt cfg msg
    }
  pure SubmitHandle { asyncSH = h.asyncEH, enqueueSH = h.enqueueEH }

kickFeeder :: SubmitConfig -> R.Feeder SubmitMsg
kickFeeder cfg q = forever $ do
  threadDelay cfg.pollIntervalMicrosSC
  atomically $ do
    _ <- Sup.tryWriteTBQueue q SubmitKick
    pure ()

--------------------------------------------------------------------------------
-- Worker

submitWorker :: Context -> SubmitConfig -> SubmitMsg -> IO ()
submitWorker ctxt cfg SubmitKick = drainLoop
  where
  drainLoop = do
    submitClaimToken <- nextRandom
    claimed <-
      claimEnteredRequests ctxt.pgPoolCT ctxt.nodeIdCT submitClaimToken cfg.batchSizeSC cfg.claimTtlSecondsSC

    if V.null claimed then
      pure ()
    else do
      processOne submitClaimToken claimed
      drainLoop

  processOne :: UUID -> Vector St.BatchRequest -> IO ()
  processOne submitClaimToken claimedRequests = do
    res <- ctxt.sendRequestCT claimedRequests
    case res of
      Left err -> releaseClaimWithError ctxt.pgPoolCT submitClaimToken err claimedRequests
      Right submitOk -> do
        -- Persist: create batch + associate requests + mark requests submitted + events.
        persistSubmittedBatch ctxt.pgPoolCT submitClaimToken submitOk.batchUidSO submitOk.providerBatchIdSO claimedRequests
        -- Fast path: poll this batch immediately.
        ctxt.enqueuePollCT submitOk.batchUidSO submitOk.providerBatchIdSO

--------------------------------------------------------------------------------
-- DB operations (all in terms of companion batch tables)

claimEnteredRequests :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector St.BatchRequest)
claimEnteredRequests pool nodeId submitClaimToken limitN ttlSec = do
  eiReqRows <- Es.execStmt "claimEnteredRequests" pool $
      Tx.statement (fromIntegral limitN, nodeId, submitClaimToken, ttlSec) Es.claimRequestsStmt
  case eiReqRows of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right reqRows ->
      let
        reqIDs = V.map fst reqRows
      in do
      eiMemories <- Es.execStmt "fetchMemories" pool $ Tx.statement reqIDs Es.fetchMemories
      case eiMemories of
        Left err -> throwIO (DbError (T.pack (show err)))
        Right memRows ->
          let
            memMap = foldl groupMemories Mp.empty memRows
            bRequests = V.map (\(reqId, msgTxt) ->
                let
                  reqContexts = Mp.findWithDefault [] reqId memMap
                in
                St.BatchRequest reqId reqContexts (NE.singleton (St.RequestMessage reqId msgTxt))
              ) reqRows
          in
          pure bRequests
          -- (V.map (uncurry ClaimedRequest) rows)
  where
  groupMemories :: Mp.Map UUID ([St.RequestContext]) -> (UUID, Int32, Value, Int32, Text, Text)
        -> Mp.Map UUID ([St.RequestContext])
  groupMemories accum (reqId, memId, metaData, seqIdx, content, cHash) =
    Mp.insertWith (<>) reqId [St.RequestContext content St.DeveloperPR] accum

-- Success path:
--  1) insert batches row
--  2) batch_events: submitted
--  3) batch_requests association (write-once)
--  4) mark each request submitted (state + clear submit-claim)
--  5) request_events: submitted (append-only)
-- submitClaimToken: for guarded request update

persistSubmittedBatch :: Pool.Pool -> UUID -> UUID -> Text -> Vector St.BatchRequest -> IO ()
persistSubmittedBatch pool submitClaimToken batchId providerBatchId requests =
  let
    detailsBatch = batchSubmittedDetails batchId providerBatchId (length requests)
  in do
  eiRez <- Es.execStmt "persistSubmittedBatch" pool $ do
    Tx.statement (batchId, providerBatchId) Es.insertBatchStmt
    Tx.statement (batchId, "submitted" :: Text, detailsBatch) Es.insertBatchEventStmt
    V.forM_ requests $ \bReq -> do
      -- association table (write-once; idempotent)
      Tx.statement (bReq.idBR, batchId, Nothing) Es.insertBatchRequestStmt
      -- request state transition (guarded by claim token)
      Tx.statement (bReq.idBR, submitClaimToken) Es.markRequestSubmittedStmt
      -- append-only request history
      Tx.statement (bReq.idBR, "submitted" :: Text, requestSubmittedDetails batchId providerBatchId) Es.insertRequestEventStmt

  case eiRez of
    Left err -> do
      putStrLn $ "@[persistSubmittedBatch] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()


releaseClaimWithError :: Pool.Pool -> UUID -> SubmitError -> Vector St.BatchRequest -> IO ()
releaseClaimWithError pool submitClaimToken submitErr requests =
  let
    details = submitFailedDetails submitErr.codeSE submitErr.messageSE
  in do
  eiRez <- Es.execStmt "releaseClaimWithError" pool $ do
    V.forM_ requests $ \bReq -> do
      Tx.statement (bReq.idBR, submitClaimToken) Es.releaseClaimStmt
      Tx.statement (bReq.idBR, "entered" :: Text, details) Es.insertRequestEventStmt
  case eiRez of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

--------------------------------------------------------------------------------
-- Event details

batchSubmittedDetails :: UUID -> Text -> Int -> Value
batchSubmittedDetails batchUid providerBatchId nReqs =
  Ae.object
    [ "event" Ae..= ("batch_submitted" :: Text)
    , "batch_uid" Ae..= batchUid
    , "provider_batch_id" Ae..= providerBatchId
    , "request_count" Ae..= nReqs
    ]

requestSubmittedDetails :: UUID -> Text -> Value
requestSubmittedDetails batchUid providerBatchId =
  Ae.object
    [ "event" Ae..= ("submitted" :: Text)
    , "batch_uid" Ae..= batchUid
    , "provider_batch_id" Ae..= providerBatchId
    ]

submitFailedDetails :: Text -> Text -> Value
submitFailedDetails code msg =
  Ae.object
    [ "event" Ae..= ("submit_failed" :: Text)
    , "error_code" Ae..= code
    , "error_message" Ae..= msg
    ]
