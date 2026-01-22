{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE QuasiQuotes #-}

module Engine.Poll where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, link, waitAnyCancel)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, void, when, unless)

import Data.Int (Int32)
import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
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
-- Public API

-- | PollStatus result: keep it minimal and map from your provider response.
data ProviderBatchStatus
  = BatchRunning
  | BatchCompleted
  | BatchCancelled Text -- reason
  | BatchFailed Text    -- reason
  deriving (Show, Eq, Generic)

data PollError = PollError
  { peCode    :: Text
  , peMessage :: Text
  }
  deriving (Show, Eq, Generic)

newtype PollEngineError
  = DbError Text
  deriving (Show, Eq, Generic, Exception)

-- | Env abstracts the provider function and the fetch queue handoff.
--   Plug Service.AiSystem.pollStatus into pollStatusCT.
data Context = Context
  { pgPoolCT      :: Pool.Pool
  , nodeIdCt      :: Text
  , pollStatusCT  :: UUID -> IO (Either PollError ProviderBatchStatus)
  , enqueueFetchCT :: UUID -> IO ()
  -- ^ Push a batch UUID into Engine.Fetch’s in-memory queue (or adapter).
  }

data PollConfig = PollConfig
  { pcPollIntervalMicros :: Int   -- how often we try to claim new batches
  , pcMaxBatchesPerTick  :: Int   -- claim up to this many batches per tick
  , pcQueueDepth         :: Int   -- bounded queue depth
  , pcWorkerCount        :: Int   -- number of poll workers
  , pcClaimTtlSeconds    :: Int32 -- minimum interval between polls of same batch
  }
  deriving (Show, Eq, Generic)

startPollEngine :: Context -> PollConfig -> IO (Async ())
startPollEngine env cfg = async (runPollEngine env cfg)

-- | Runs forever until killed:
--   - 1 claimer thread: claims eligible batches and enqueues jobs
--   - N worker threads: calls provider pollStatus and updates DB + signals fetch
runPollEngine :: Context -> PollConfig -> IO ()
runPollEngine ctxt cfg = do
  q <- newTBQueueIO (fromIntegral cfg.pcQueueDepth)

  claimerA <- async (claimerLoop ctxt cfg q)
  workersA <- mapM (\i -> async (workerLoop ctxt cfg i q)) [1 .. cfg.pcWorkerCount]

  link claimerA
  mapM_ link workersA

  void $ waitAnyCancel (claimerA : workersA)

--------------------------------------------------------------------------------
-- Claimer

data PollJob = PollJob
  { pjClaimToken :: UUID
  , pjBatchUuid  :: UUID
  }
  deriving (Show, Eq, Generic)

claimerLoop :: Context -> PollConfig -> TBQueue PollJob -> IO ()
claimerLoop ctxt pollCfg q = forever $ do
  token <- nextRandom
  batches <- claimSubmittedBatches ctxt.pgPoolCT ctxt.nodeIdCt token pollCfg.pcMaxBatchesPerTick pollCfg.pcClaimTtlSeconds
  if V.null batches then
    threadDelay pollCfg.pcPollIntervalMicros
  else
    atomically $ mapM_ (writeTBQueue q . PollJob token) batches

-- Claim distinct batches with submitted requests, and lease them for pcClaimTtlSeconds.
claimSubmittedBatches
  :: Pool.Pool
  -> Text      -- node id
  -> UUID      -- claim token
  -> Int       -- limit batches
  -> Int32     -- ttl seconds
  -> IO (Vector UUID)
claimSubmittedBatches pool nodeId token limitB ttlSec = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
          Tx.statement (fromIntegral limitB, nodeId, token, ttlSec) claimBatchesStmt

  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector


-- Returns one row per claimed batch UUID.
claimBatchesStmt :: Statement (Int32, Text, UUID, Int32) (Vector UUID)
claimBatchesStmt =
  [vectorStatement|
    with picked as (
      select distinct on (r.provider_batch_uuid)
             r.request_id,
             r.provider_batch_uuid
        from batcher.requests r
       where r.state = 'submitted'
         and r.provider_batch_uuid is not null
         and (r.poll_claimed_until is null or r.poll_claimed_until < now())
       order by r.provider_batch_uuid, r.updated_at asc
       limit $1 :: int4
       for update skip locked
    ),
    mark_one as (
      update batcher.requests r
         set poll_claimed_by    = $2 :: text,
             poll_claim_token   = $3 :: uuid,
             poll_claimed_until = now() + make_interval(secs => $4 :: int4),
             updated_at         = now()
        from picked p
       where r.request_id = p.request_id
       returning p.provider_batch_uuid
    ),
    mark_all as (
      update batcher.requests r
         set poll_claimed_by    = $2 :: text,
             poll_claim_token   = $3 :: uuid,
             poll_claimed_until = now() + make_interval(secs => $4 :: int4),
             updated_at         = now()
        from mark_one m
       where r.provider_batch_uuid = m.provider_batch_uuid
       returning 1
    )
    select provider_batch_uuid :: uuid
      from mark_one
  |]

--------------------------------------------------------------------------------
-- Worker

workerLoop :: Context -> PollConfig -> Int -> TBQueue PollJob -> IO ()
workerLoop ctxt pollCfg workerIx q = forever $ do
  pollJob <- atomically (readTBQueue q)

  res <- ctxt.pollStatusCT pollJob.pjBatchUuid
  case res of
    Left _err -> do
      -- Provider call failed: release claim early so we can retry sooner than TTL.
      -- (If you prefer throttling on errors too, remove this release.)
      releasePollClaim ctxt.pgPoolCT pollJob.pjClaimToken pollJob.pjBatchUuid

    Right BatchRunning ->
      -- Do nothing: the lease TTL already throttles re-poll frequency.
      pure ()

    Right BatchCompleted -> do
      reqIds <- markBatchCompleted ctxt.pgPoolCT pollJob.pjClaimToken pollJob.pjBatchUuid
      unless (V.null reqIds) $ do
        insertFetchOutbox ctxt.pgPoolCT pollJob.pjBatchUuid
        ctxt.enqueueFetchCT pollJob.pjBatchUuid

    Right (BatchCancelled reason) -> do
      _ <- markBatchCancelled ctxt.pgPoolCT pollJob.pjClaimToken pollJob.pjBatchUuid "cancelled" reason
      pure ()

    Right (BatchFailed reason) -> do
      _ <- markBatchCancelled ctxt.pgPoolCT pollJob.pjClaimToken pollJob.pjBatchUuid "failed" reason
      pure ()

--------------------------------------------------------------------------------
-- DB ops: state transitions + events + fetch outbox

-- Mark all submitted requests in this batch as completed.
-- Returns request_ids that were transitioned.
markBatchCompleted :: Pool.Pool -> UUID -> UUID -> IO (Vector UUID)
markBatchCompleted pool token batchUuid = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $ do
      reqIds <- Tx.statement (batchUuid, token) markBatchCompletedStmt
      mapM_ (\rid ->
                Tx.statement (rid, "completed" :: Text, completedDetails batchUuid)
                  insertRequestEventStmt
            ) reqIds
      -- clear poll claim so completed rows aren't "stuck" claimed
      Tx.statement (batchUuid, token) clearPollClaimBatchStmt
      pure reqIds

  eiRez <- Pool.use pool session
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right aVector -> pure aVector


completedDetails :: UUID -> Value
completedDetails batchUuid = Ae.object [
      "event" Ae..= ("provider_completed" :: Text)
    , "provider_batch_uuid" Ae..= batchUuid
    ]

-- Mark as cancelled (covers cancelled OR failed; state becomes 'cancelled').
markBatchCancelled :: Pool.Pool -> UUID -> UUID -> Text -> Text -> IO (Vector UUID)
markBatchCancelled pool token batchUuid kind reason = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $ do
      reqIds <- Tx.statement (batchUuid, token) markBatchCancelledStmt
      mapM_ (\rid ->
            Tx.statement (rid, "cancelled" :: Text, cancelledDetails batchUuid kind reason)
              insertRequestEventStmt
        ) reqIds
      Tx.statement (batchUuid, token) clearPollClaimBatchStmt
      pure reqIds

  eiRez <- Pool.use pool session
  case eiRez of
    Left e  -> throwIO (DbError (T.pack (show e)))
    Right aVector -> pure aVector


cancelledDetails :: UUID -> Text -> Text -> Value
cancelledDetails batchUuid kind reason =
  Ae.object
    [ "event" Ae..= ("provider_terminal" :: Text)
    , "terminal_kind" Ae..= kind          -- "cancelled" | "failed"
    , "reason" Ae..= reason
    , "provider_batch_uuid" Ae..= batchUuid
    ]


-- Durable outbox insert (idempotent).
insertFetchOutbox :: Pool.Pool -> UUID -> IO ()
insertFetchOutbox pool batchUuid = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
          Tx.statement batchUuid insertFetchOutboxStmt
  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()

-- Release claim early on provider call error.
releasePollClaim :: Pool.Pool -> UUID -> UUID -> IO ()
releasePollClaim pool token batchUuid = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
          Tx.statement (batchUuid, token) clearPollClaimBatchStmt
  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()

--------------------------------------------------------------------------------
-- Statements

markBatchCompletedStmt :: Statement (UUID, UUID) (Vector UUID)
markBatchCompletedStmt =
  [vectorStatement|
    update batcher.requests
       set state      = 'completed',
           updated_at = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'submitted'
       and poll_claim_token   = $2 :: uuid
     returning request_id :: uuid
  |]

markBatchCancelledStmt :: Statement (UUID, UUID) (Vector UUID)
markBatchCancelledStmt =
  [vectorStatement|
    update batcher.requests
       set state      = 'cancelled',
           updated_at = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'submitted'
       and poll_claim_token   = $2 :: uuid
     returning request_id :: uuid
  |]

clearPollClaimBatchStmt :: Statement (UUID, UUID) ()
clearPollClaimBatchStmt =
  [resultlessStatement|
    update batcher.requests
       set poll_claimed_until = null,
           poll_claimed_by    = null,
           poll_claim_token   = null,
           updated_at         = now()
     where provider_batch_uuid = $1 :: uuid
       and poll_claim_token    = $2 :: uuid
  |]

insertRequestEventStmt :: Statement (UUID, Text, Value) ()
insertRequestEventStmt =
  [resultlessStatement|
    insert into batcher.request_events
      (request_id, state, details)
    values
      ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]

insertFetchOutboxStmt :: Statement UUID ()
insertFetchOutboxStmt =
  [resultlessStatement|
    insert into batcher.fetch_outbox (provider_batch_uuid)
    values ($1 :: uuid)
    on conflict (provider_batch_uuid) do nothing
  |]
