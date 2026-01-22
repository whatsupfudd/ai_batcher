{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}

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
  { scPollIntervalMicros :: Int
  , scBatchSize          :: Int
  , scQueueDepth         :: Int
  , scWorkerCount        :: Int
  , scClaimTtlSeconds    :: Int32
  }
  deriving (Show, Eq, Generic)

data SubmitOutcome
  = SubmitOkOutcome
  | SubmitFailedOutcome
  deriving (Show, Eq, Generic)

-- Returned by your AI service integration layer (adapt as needed).
data SubmitOk = SubmitOk
  { soProviderBatchId    :: Text
  , soProviderRequestIds :: [(UUID, Text)]
  -- ^ Optional mapping (request_id -> provider_request_id).
  }
  deriving (Show, Eq, Generic)

data SubmitError = SubmitError
  { seCode    :: Text
  , seMessage :: Text
  }
  deriving (Show, Eq, Generic)

newtype SubmitEngineError
  = DbError Text
  deriving (Show, Eq, Generic, Exception)

--------------------------------------------------------------------------------
-- Internal job model

data ClaimedRequest = ClaimedRequest
  { crRequestId   :: UUID
  , crRequestText :: Text
  }
  deriving (Show, Eq, Generic)

data SubmitJob = SubmitJob
  { sjClaimToken :: UUID
  , sjRequests   :: NonEmpty ClaimedRequest
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
runSubmitEngine ctxt submitCfg = do
  q <- newTBQueueIO (fromIntegral submitCfg.scQueueDepth)

  pollerA <- async (pollerLoop ctxt submitCfg q)
  workers <- mapM (\i -> async (workerLoop ctxt submitCfg i q)) [1 .. submitCfg.scWorkerCount]

  -- Fail-fast: if any thread dies, kill the whole engine.
  link pollerA
  mapM_ link workers

  -- Block until someone dies; then cancel others.
  void $ waitAnyCancel (pollerA : workers)

--------------------------------------------------------------------------------
-- Poller

pollerLoop :: Context -> SubmitConfig -> TBQueue SubmitJob -> IO ()
pollerLoop ctxt submitCfg q = forever $ do
  claimToken <- nextRandom
  claimed <- claimEnteredRequests ctxt.pgPoolCT ctxt.nodeIdCT claimToken submitCfg.scBatchSize submitCfg.scClaimTtlSeconds

  if V.null claimed then
    threadDelay submitCfg.scPollIntervalMicros
  else
    let
      neList = NE.fromList (V.toList claimed)
    in
    atomically (writeTBQueue q (SubmitJob claimToken neList))

-- Claim rows with TTL so multiple engines/nodes don't double-submit.
-- node id, claim token, limit, ttl seconds
claimEnteredRequests :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector ClaimedRequest)
claimEnteredRequests pool nodeId token limitN ttlSec = do
  let
    session =
        TxS.transaction TxS.ReadCommitted TxS.Write $ do
          rows <- Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) claimRequestsStmt
          pure (V.map (uncurry ClaimedRequest) rows)

  eiRez <- Pool.use pool session
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right x -> pure x

-- Returns (request_id, request_text)
claimRequestsStmt :: Statement (Int32, Text, UUID, Int32) (Vector (UUID, Text))
claimRequestsStmt =
  [vectorStatement|
    with picked as (
      select r.request_id, r.request_text
      from batcher.requests r
      where r.state = 'entered'
        and (r.submit_claimed_until is null or r.submit_claimed_until < now())
      order by r.created_at asc
      limit $1 :: int4
      for update skip locked
    )
    update batcher.requests u
      set submit_claimed_by    = $2 :: text,
          submit_claim_token   = $3 :: uuid,
          submit_claimed_until = now() + make_interval(secs => $4 :: int4),
          updated_at           = now()
    from picked
    where u.request_id = picked.request_id
    returning u.request_id :: uuid, picked.request_text :: text
  |]

--------------------------------------------------------------------------------
-- Worker

workerLoop :: Context -> SubmitConfig -> Int -> TBQueue SubmitJob -> IO ()
workerLoop ctxt submitCfg workerIx q = forever $ do
  job <- atomically (readTBQueue q)
  let reqPairs :: NonEmpty (UUID, Text)
      reqPairs =
        NE.map (\ClaimedRequest{..} -> (crRequestId, crRequestText)) (sjRequests job)

  outcome <- ctxt.sendRequestCT reqPairs

  case outcome of
    Right SubmitOk{..} -> do
      markSubmittedBatch ctxt.pgPoolCT (sjClaimToken job) soProviderBatchId soProviderRequestIds (NE.toList (sjRequests job))
    Left err -> do
      releaseClaimWithError ctxt.pgPoolCT (sjClaimToken job) err (NE.toList (sjRequests job))

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
    session =
        TxS.transaction TxS.ReadCommitted TxS.Write $ do
          -- update + event per request (simple and reliable for v1; optimize later if needed)
          mapM_ (\ClaimedRequest{..} -> do
                Tx.statement (crRequestId, token, providerBatchId, lookupProv crRequestId) markSubmittedStmt
                Tx.statement (crRequestId, "submitted" :: Text, submittedDetails providerBatchId (lookupProv crRequestId))
                  insertRequestEventStmt
            ) reqs

  r <- Pool.use pool session
  case r of
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
releaseClaimWithError pool token SubmitError{..} reqs = do
  let session =
        TxS.transaction TxS.ReadCommitted TxS.Write $ do
          mapM_ (\ClaimedRequest{..} -> do
                Tx.statement (crRequestId, token) releaseClaimStmt
                Tx.statement (crRequestId, "entered" :: Text, failureDetails seCode seMessage)
                  insertRequestEventStmt
            ) reqs

  eiRez <- Pool.use pool session
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right _ -> pure ()


failureDetails :: Text -> Text -> Value
failureDetails code msg = Ae.object [ 
      "event" Ae..= ("submit_failed" :: Text)
    , "error_code" Ae..= code
    , "error_message" Ae..= msg
    ]

--------------------------------------------------------------------------------
-- DB statements

-- Only updates if claim token matches (prevents stale worker updates).
markSubmittedStmt :: Statement (UUID, UUID, Text, Maybe Text) ()
markSubmittedStmt =
  [resultlessStatement|
    update batcher.requests set
      state = 'submitted',
      provider_batch_id = $3 :: text,
      provider_request_id = $4 :: text?,
      submit_claimed_until = null,
      submit_claimed_by = null,
      submit_claim_token = null,
      updated_at = now()
     where request_id = $1 :: uuid
       and submit_claim_token = $2 :: uuid
  |]


releaseClaimStmt :: Statement (UUID, UUID) ()
releaseClaimStmt =
  [resultlessStatement|
    update batcher.requests set 
      submit_claimed_until = null,
      submit_claimed_by = null,
      submit_claim_token = null,
      updated_at = now()
     where request_id = $1 :: uuid
       and submit_claim_token = $2 :: uuid
  |]


insertRequestEventStmt :: Statement (UUID, Text, Value) ()
insertRequestEventStmt =
  [resultlessStatement|
    insert into batcher.request_events
      (request_id, state, details)
    values
      ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]
