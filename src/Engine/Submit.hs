{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Engine.Submit where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, cancel, link, waitAnyCancel)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, unless, void, when)

import Data.Int (Int32, Int64)
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock (NominalDiffTime)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)
import qualified Data.Vector as V

import GHC.Generics (Generic)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS
import Hasql.Statement (Statement)
import Hasql.TH

import qualified DB.EngineStmt as Es

--------------------------------------------------------------------------------
-- Public types

-- | Plug your existing sender here.
--   We keep it abstract because signatures vary across implementations.
--
--   Suggested behavior:
--     - input: batch of (request_id, request_text)
--     - output: provider batch id + optional per-request provider ids
--
--   If your provider returns only a batch id, return [] for per-request ids.
data Context = Context
  { pgPoolCT      :: Pool.Pool
  , nodeIdCT      :: Text
  , sendRequestCT :: NonEmpty (UUID, Text) -> IO (Either SubmitError SubmitOk)
  }

data SubmitConfig = SubmitConfig
  { pollIntervalMicrosSC :: Int
  , batchSizeSC          :: Int
  , queueDepthSC         :: Int
  , workerCountSC        :: Int
  , claimTtlSecondsSC    :: Int32
  }
  deriving (Show, Eq, Generic)

data SubmitOutcome
  = SubmitOkOutcome
  | SubmitFailedOutcome
  deriving (Show, Eq, Generic)

-- Returned by your AI service integration layer (adapt as needed).
data SubmitOk = SubmitOk
  { providerBatchIDSO    :: Text
  , providerRequestIDsSO :: [(UUID, Text)]
  -- ^ Optional mapping (request_id -> provider_request_id).
  }
  deriving (Show, Eq, Generic)

data SubmitError = SubmitError
  {  codeSE    :: Text
  , messageSE :: Text
  }
  deriving (Show, Eq, Generic)

newtype SubmitEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

--------------------------------------------------------------------------------
-- Internal job model

data ClaimedRequest = ClaimedRequest
  { requestIDCR   :: UUID
  , requestTextCR :: Text
  }
  deriving (Show, Eq, Generic)

data SubmitJob = SubmitJob
  { claimTokenSJ :: UUID
  , requestsSJ   :: NonEmpty ClaimedRequest
  }
  deriving (Show, Eq, Generic)

--------------------------------------------------------------------------------
-- Engine lifecycle

startSubmitEngine :: Context -> SubmitConfig -> IO (Async ())
startSubmitEngine env cfg = async (runSubmitEngine env cfg)

-- | Runs forever (until killed). Spawns:
--   - 1 poller thread (claims rows and enqueues SubmitJob)
--   - N worker threads (consume queue and call sendRequestCT)
runSubmitEngine :: Context -> SubmitConfig -> IO ()
runSubmitEngine ctxt cfg = do
  q <- newTBQueueIO (fromIntegral cfg.queueDepthSC)

  pollerA <- async (pollerLoop ctxt cfg q)
  workers <- mapM (\i -> async (workerLoop ctxt cfg i q)) [1 .. cfg.workerCountSC]

  -- Fail-fast: if any thread dies, kill the whole engine.
  link pollerA
  mapM_ link workers

  -- Block until someone dies; then cancel others.
  void $ waitAnyCancel (pollerA : workers)

--------------------------------------------------------------------------------
-- Poller

pollerLoop :: Context -> SubmitConfig -> TBQueue SubmitJob -> IO ()
pollerLoop ctxt cfg q = forever $ do
  claimToken <- nextRandom
  claimed <- claimEnteredRequests ctxt.pgPoolCT ctxt.nodeIdCT claimToken cfg.batchSizeSC cfg.claimTtlSecondsSC

  if V.null claimed then
    threadDelay cfg.pollIntervalMicrosSC
  else
    let
      neList = NE.fromList (V.toList claimed)
    in
    atomically (writeTBQueue q (SubmitJob claimToken neList))

-- Claim rows with TTL so multiple engines/nodes don't double-submit.
-- node id, claim token, limit, ttl seconds
claimEnteredRequests :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector ClaimedRequest)
claimEnteredRequests pool nodeId token limitN ttlSec = do
  eiRez <- Es.execStmt pool $ do
            rows <- Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) Es.claimRequestsStmt
            pure (V.map (uncurry ClaimedRequest) rows)

  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right x -> pure x


--------------------------------------------------------------------------------
-- Worker
workerLoop :: Context -> SubmitConfig -> Int -> TBQueue SubmitJob -> IO ()
workerLoop ctxt cfg workerIx q = forever $ do
  job <- atomically (readTBQueue q)
  let reqPairs :: NonEmpty (UUID, Text)
      reqPairs =
        NE.map (\claimedReq -> (claimedReq.requestIDCR, claimedReq.requestTextCR)) job.requestsSJ

  outcome <- ctxt.sendRequestCT reqPairs

  case outcome of
    Right submitOk -> do
      markSubmittedBatch ctxt.pgPoolCT job.claimTokenSJ submitOk.providerBatchIDSO submitOk.providerRequestIDsSO (NE.toList job.requestsSJ)
    Left err -> do
      releaseClaimWithError ctxt.pgPoolCT job.claimTokenSJ err (NE.toList job.requestsSJ)


-- Success path: set state=submitted, store provider ids, clear claim, write events.
markSubmittedBatch
  :: Pool.Pool
  -> UUID              -- claim token
  -> Text              -- provider batch id
  -> [(UUID, Text)]    -- provider per-request ids (optional)
  -> [ClaimedRequest]
  -> IO ()
markSubmittedBatch pool token providerBatchId providerReqIds reqs = do
  let
    lookupProv rid = lookup rid providerReqIds

  eiRez <- Es.execStmt pool $ do
          -- update + event per request (simple and reliable for v1; optimize later if needed)
          mapM_ (\claimedReq -> do
                Tx.statement (claimedReq.requestIDCR, token, providerBatchId, lookupProv claimedReq.requestIDCR) Es.markSubmittedStmt
                Tx.statement (claimedReq.requestIDCR, "submitted" :: Text, submittedDetails providerBatchId (lookupProv claimedReq.requestIDCR))
                  Es.insertRequestEventStmt
            ) reqs
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right _ -> pure ()


submittedDetails :: Text -> Maybe Text -> Value
submittedDetails batchId mReqId =
  Ae.object $
    [ "event" Ae..= ("submitted" :: Text)
    , "provider_batch_id" Ae..= batchId
    ] <> case mReqId of
           Nothing -> []
           Just aText  -> ["provider_request_id" Ae..= aText]


-- Failure path: clear claim, keep state=entered, write an event noting the failure.
releaseClaimWithError
  :: Pool.Pool
  -> UUID
  -> SubmitError
  -> [ClaimedRequest]
  -> IO ()
releaseClaimWithError pool token submitErr reqs = do
  eiRez <- Es.execStmt pool $ do
          mapM_ (\claimedReq -> do
                Tx.statement (claimedReq.requestIDCR, token) Es.releaseClaimStmt
                Tx.statement (claimedReq.requestIDCR, "entered" :: Text, failureDetails submitErr.codeSE submitErr.messageSE)
                  Es.insertRequestEventStmt
            ) reqs
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right _ -> pure ()


failureDetails :: Text -> Text -> Value
failureDetails code msg = Ae.object [ 
      "event" Ae..= ("submit_failed" :: Text)
    , "error_code" Ae..= code
    , "error_message" Ae..= msg
    ]
