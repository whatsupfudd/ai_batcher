{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

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

import qualified DB.EngineStmt as Es

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
  { codePE    :: Text
  , messagePE :: Text
  }
  deriving (Show, Eq, Generic)

newtype PollEngineError = DbError Text
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
  { pollIntervalMicrosPC :: Int   -- how often we try to claim new batches
  , maxBatchesPerTickPC  :: Int   -- claim up to this many batches per tick
  , queueDepthPC         :: Int   -- bounded queue depth
  , workerCountPC        :: Int   -- number of poll workers
  , claimTtlSecondsPC    :: Int32 -- minimum interval between polls of same batch
  }
  deriving (Show, Eq, Generic)

startPollEngine :: Context -> PollConfig -> IO (Async ())
startPollEngine env cfg = async (runPollEngine env cfg)

-- | Runs forever until killed:
--   - 1 claimer thread: claims eligible batches and enqueues jobs
--   - N worker threads: calls provider pollStatus and updates DB + signals fetch
runPollEngine :: Context -> PollConfig -> IO ()
runPollEngine ctxt cfg = do
  q <- newTBQueueIO (fromIntegral cfg.queueDepthPC)

  claimerA <- async (claimerLoop ctxt cfg q)
  workersA <- mapM (\i -> async (workerLoop ctxt cfg i q)) [1 .. cfg.workerCountPC]

  link claimerA
  mapM_ link workersA

  void $ waitAnyCancel (claimerA : workersA)

--------------------------------------------------------------------------------
-- Claimer

data PollJob = PollJob
  { claimTokenPJ :: UUID
  , batchUuidPJ  :: UUID
  }
  deriving (Show, Eq, Generic)


claimerLoop :: Context -> PollConfig -> TBQueue PollJob -> IO ()
claimerLoop ctxt pollCfg q = forever $ do
  token <- nextRandom
  batches <- claimSubmittedBatches ctxt.pgPoolCT ctxt.nodeIdCt token pollCfg.maxBatchesPerTickPC pollCfg.claimTtlSecondsPC
  if V.null batches then
    threadDelay pollCfg.pollIntervalMicrosPC
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
  eiRez <- Es.execStmt pool $
          Tx.statement (fromIntegral limitB, nodeId, token, ttlSec) Es.claimBatchesStmt

  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector

--------------------------------------------------------------------------
-- Worker

workerLoop :: Context -> PollConfig -> Int -> TBQueue PollJob -> IO ()
workerLoop ctxt pollCfg workerIx q = forever $ do
  pollJob <- atomically (readTBQueue q)

  res <- ctxt.pollStatusCT pollJob.batchUuidPJ
  case res of
    Left _err -> do
      -- Provider call failed: release claim early so we can retry sooner than TTL.
      -- (If you prefer throttling on errors too, remove this release.)
      releasePollClaim ctxt.pgPoolCT pollJob.claimTokenPJ pollJob.batchUuidPJ

    Right BatchRunning ->
      -- Do nothing: the lease TTL already throttles re-poll frequency.
      pure ()

    Right BatchCompleted -> do
      reqIds <- markBatchCompleted ctxt.pgPoolCT pollJob.claimTokenPJ pollJob.batchUuidPJ
      unless (V.null reqIds) $ do
        insertFetchOutbox ctxt.pgPoolCT pollJob.batchUuidPJ
        ctxt.enqueueFetchCT pollJob.batchUuidPJ

    Right (BatchCancelled reason) -> do
      _ <- markBatchCancelled ctxt.pgPoolCT pollJob.claimTokenPJ pollJob.batchUuidPJ "cancelled" reason
      pure ()

    Right (BatchFailed reason) -> do
      _ <- markBatchCancelled ctxt.pgPoolCT pollJob.claimTokenPJ pollJob.batchUuidPJ "failed" reason
      pure ()

--------------------------------------------------------------------------------
-- DB ops: state transitions + events + fetch outbox

-- Mark all submitted requests in this batch as completed.
-- Returns request_ids that were transitioned.
markBatchCompleted :: Pool.Pool -> UUID -> UUID -> IO (Vector UUID)
markBatchCompleted pool token batchUuid = do
  eiRez <- Es.execStmt pool $ do
      reqIds <- Tx.statement (batchUuid, token) Es.markBatchCompletedStmt
      mapM_ (\rid ->
                Tx.statement (rid, "completed" :: Text, completedDetails batchUuid)
                  Es.insertRequestEventStmt
            ) reqIds
      -- clear poll claim so completed rows aren't "stuck" claimed
      Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
      pure reqIds

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
  eiRez <- Es.execStmt pool $ do
      reqIds <- Tx.statement (batchUuid, token) Es.markBatchCancelledStmt
      mapM_ (\rid ->
            Tx.statement (rid, "cancelled" :: Text, cancelledDetails batchUuid kind reason)
              Es.insertRequestEventStmt
        ) reqIds
      Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
      pure reqIds
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
  eiRez <- Es.execStmt pool $
          Tx.statement batchUuid Es.insertFetchOutboxStmt
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()

-- Release claim early on provider call error.
releasePollClaim :: Pool.Pool -> UUID -> UUID -> IO ()
releasePollClaim pool token batchUuid = do
  eiRez <- Es.execStmt pool $
          Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()
