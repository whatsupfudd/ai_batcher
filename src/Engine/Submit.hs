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
import Control.Monad (forever)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae
import Data.Int (Int32)
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

--------------------------------------------------------------------------------
-- Public API

data Context = Context
  { pgPoolCT      :: Pool.Pool
  , nodeIdCT      :: Text
  , sendRequestCT :: NonEmpty (UUID, Text) -> IO (Either SubmitError SubmitOk)
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
      claimed <- claimEnteredRequests ctxt.pgPoolCT ctxt.nodeIdCT submitClaimToken
                        cfg.batchSizeSC cfg.claimTtlSecondsSC

      if V.null claimed
        then pure ()
        else do
          processOne submitClaimToken claimed
          drainLoop

    processOne submitClaimToken claimedVec = do
      let claimedList = V.toList claimedVec
          reqPairs :: NonEmpty (UUID, Text)
          reqPairs =
            NE.fromList (map (\cr -> (requestIdCR cr, requestTextCR cr)) claimedList)

      res <- ctxt.sendRequestCT reqPairs
      case res of
        Left err ->
          releaseClaimWithError ctxt.pgPoolCT submitClaimToken err claimedList

        Right submitOk -> do
          -- Persist: create batch + associate requests + mark requests submitted + events.
          persistSubmittedBatch ctxt.pgPoolCT submitClaimToken submitOk.batchUidSO
                  submitOk.providerBatchIdSO claimedList

          -- Fast path: poll this batch immediately.
          ctxt.enqueuePollCT submitOk.batchUidSO submitOk.providerBatchIdSO

--------------------------------------------------------------------------------
-- DB operations (all in terms of companion batch tables)

claimEnteredRequests
  :: Pool.Pool
  -> Text
  -> UUID
  -> Int
  -> Int32
  -> IO (Vector ClaimedRequest)
claimEnteredRequests pool nodeId submitClaimToken limitN ttlSec = do
  ei <-
    Es.execStmt pool $
      Tx.statement
        (fromIntegral limitN, nodeId, submitClaimToken, ttlSec)
        Es.claimRequestsStmt
  case ei of
    Left err   -> throwIO (DbError (T.pack (show err)))
    Right rows -> pure (V.map (uncurry ClaimedRequest) rows)

-- Success path:
--  1) insert batches row
--  2) batch_events: submitted
--  3) batch_requests association (write-once)
--  4) mark each request submitted (state + clear submit-claim)
--  5) request_events: submitted (append-only)
persistSubmittedBatch
  :: Pool.Pool
  -> UUID       -- submit claim token (for guarded request update)
  -> UUID       -- batch uid (our UUID)
  -> Text       -- provider batch id (text)
  -> [ClaimedRequest]
  -> IO ()
persistSubmittedBatch pool submitClaimToken batchUid providerBatchId reqs = do
  let
    detailsBatch = batchSubmittedDetails batchUid providerBatchId (length reqs)

  eiRez <- Es.execStmt pool $ do
    Tx.statement (batchUid, providerBatchId) Es.insertBatchStmt
    Tx.statement (batchUid, "submitted" :: Text, detailsBatch) Es.insertBatchEventStmt

    V.forM_ (V.fromList reqs) $ \cr -> do
      -- association table (write-once; idempotent)
      Tx.statement (requestIdCR cr, batchUid, Nothing) Es.insertBatchRequestStmt

      -- request state transition (guarded by claim token)
      Tx.statement (requestIdCR cr, submitClaimToken) Es.markRequestSubmittedStmt

      -- append-only request history
      Tx.statement
        (requestIdCR cr, "submitted" :: Text, requestSubmittedDetails batchUid providerBatchId)
        Es.insertRequestEventStmt

  case eiRez of
    Left err -> do
      putStrLn $ "@[persistSubmittedBatch] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

releaseClaimWithError
  :: Pool.Pool
  -> UUID
  -> SubmitError
  -> [ClaimedRequest]
  -> IO ()
releaseClaimWithError pool submitClaimToken submitErr reqs = do
  let details = submitFailedDetails submitErr.codeSE submitErr.messageSE

  ei <- Es.execStmt pool $ do
    V.forM_ (V.fromList reqs) $ \cr -> do
      Tx.statement (requestIdCR cr, submitClaimToken) Es.releaseClaimStmt
      Tx.statement (requestIdCR cr, "entered" :: Text, details) Es.insertRequestEventStmt

  case ei of
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
