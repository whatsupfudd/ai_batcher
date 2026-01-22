{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Engine.Poll
  ( Context(..)
  , PollConfig(..)
  , ProviderBatchStatus(..)
  , PollError(..)
  , PollHandle(..)
  , startPollEngine
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, unless, void, when)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae
import Data.Int (Int32)
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

data ProviderBatchStatus
  = BatchRunning
  | BatchCompleted
  | BatchCancelled Text
  | BatchFailed Text
  deriving (Show, Eq, Generic)

data PollError = PollError
  { codePE    :: Text
  , messagePE :: Text
  }
  deriving (Show, Eq, Generic)

newtype PollEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

data Context = Context
  { pgPoolCT        :: Pool.Pool
  , nodeIdCT        :: Text
  , pollStatusCT    :: UUID -> IO (Either PollError ProviderBatchStatus)
  , enqueueFetchCT  :: UUID -> IO ()
  }

data PollConfig = PollConfig
  { pollIntervalMicrosPC :: Int
  , maxBatchesPerTickPC  :: Int
  , queueDepthPC         :: Int
  , workerCountPC        :: Int
  , claimTtlSecondsPC    :: Int32
  }
  deriving (Show, Eq, Generic)

data PollHandle = PollHandle
  { asyncPH   :: Async ()
  , enqueuePH :: UUID -> IO ()
  }

data PollJob = PollJob
  { batchUuidPJ  :: UUID
  , claimTokenPJ :: Maybe UUID
  }
  deriving (Show, Eq, Generic)

startPollEngine :: Context -> PollConfig -> IO PollHandle
startPollEngine ctxt cfg = do
  h <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthPC
    , workerCountES = cfg.workerCountPC
    , feedersES     = [claimerFeeder ctxt cfg]
    , workerES      = \_i job -> pollWorker ctxt cfg job
    }
  pure PollHandle
    { asyncPH = h.asyncEH
    , enqueuePH = \batchUuid -> h.enqueueEH (PollJob batchUuid Nothing)
    }

claimerFeeder :: Context -> PollConfig -> R.Feeder PollJob
claimerFeeder ctxt cfg q = forever $ do
  token <- nextRandom
  batches <- claimSubmittedBatches
              ctxt.pgPoolCT
              ctxt.nodeIdCT
              token
              cfg.maxBatchesPerTickPC
              cfg.claimTtlSecondsPC
  if V.null batches then
    threadDelay cfg.pollIntervalMicrosPC
  else
    atomically $
      V.forM_ batches (\b -> writeTBQueue q (PollJob b (Just token)))

claimSubmittedBatches :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector UUID)
claimSubmittedBatches pool nodeId token limitB ttlSec = do
  ei <- Es.execStmt pool $
          Tx.statement (fromIntegral limitB, nodeId, token, ttlSec) Es.claimBatchesStmt
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right v  -> pure v

-- NEW: claim one specific batch (fast-path jobs)
claimSubmittedBatchOne :: Pool.Pool -> Text -> UUID -> UUID -> Int32 -> IO Bool
claimSubmittedBatchOne pool nodeId token batchUuid ttlSec = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUuid, nodeId, token, ttlSec) Es.claimPollBatchOneStmt
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right v  -> pure (not (V.null v))

pollWorker :: Context -> PollConfig -> PollJob -> IO ()
pollWorker ctxt cfg job = do
  (token, claimed) <- case job.claimTokenPJ of
    Just t  -> pure (t, True)
    Nothing -> do
      t <- nextRandom
      ok <- claimSubmittedBatchOne ctxt.pgPoolCT ctxt.nodeIdCT t job.batchUuidPJ cfg.claimTtlSecondsPC
      pure (t, ok)

  unless claimed (pure ())  -- already claimed elsewhere / not eligible

  when claimed $ do
    st <- ctxt.pollStatusCT job.batchUuidPJ
    case st of
      Left _ -> do
        -- early release to retry sooner
        releasePollClaim ctxt.pgPoolCT token job.batchUuidPJ

      Right BatchRunning ->
        pure () -- keep TTL claim

      Right BatchCompleted -> do
        reqIds <- markBatchCompleted ctxt.pgPoolCT token job.batchUuidPJ
        unless (V.null reqIds) $ do
          insertFetchOutbox ctxt.pgPoolCT job.batchUuidPJ
          ctxt.enqueueFetchCT job.batchUuidPJ

      Right (BatchCancelled reason) -> do
        void $ markBatchCancelled ctxt.pgPoolCT token job.batchUuidPJ "cancelled" reason

      Right (BatchFailed reason) -> do
        void $ markBatchCancelled ctxt.pgPoolCT token job.batchUuidPJ "failed" reason

markBatchCompleted :: Pool.Pool -> UUID -> UUID -> IO (Vector UUID)
markBatchCompleted pool token batchUuid = do
  ei <- Es.execStmt pool $ do
          reqIds <- Tx.statement (batchUuid, token) Es.markBatchCompletedStmt
          V.forM_ reqIds (\rid ->
            Tx.statement (rid, "completed" :: Text, completedDetails batchUuid) Es.insertRequestEventStmt
            )
          Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
          pure reqIds
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right v  -> pure v

completedDetails :: UUID -> Value
completedDetails batchUuid =
  Ae.object
    [ "event" Ae..= ("provider_completed" :: Text)
    , "provider_batch_uuid" Ae..= batchUuid
    ]

markBatchCancelled :: Pool.Pool -> UUID -> UUID -> Text -> Text -> IO (Vector UUID)
markBatchCancelled pool token batchUuid kind reason = do
  ei <- Es.execStmt pool $ do
          reqIds <- Tx.statement (batchUuid, token) Es.markBatchCancelledStmt
          V.forM_ reqIds (\rid ->
            Tx.statement (rid, "cancelled" :: Text, cancelledDetails batchUuid kind reason) Es.insertRequestEventStmt
            )
          Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
          pure reqIds
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right v  -> pure v

cancelledDetails :: UUID -> Text -> Text -> Value
cancelledDetails batchUuid kind reason =
  Ae.object
    [ "event" Ae..= ("provider_terminal" :: Text)
    , "terminal_kind" Ae..= kind
    , "reason" Ae..= reason
    , "provider_batch_uuid" Ae..= batchUuid
    ]

insertFetchOutbox :: Pool.Pool -> UUID -> IO ()
insertFetchOutbox pool batchUuid = do
  ei <- Es.execStmt pool $
          Tx.statement batchUuid Es.insertFetchOutboxStmt
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

releasePollClaim :: Pool.Pool -> UUID -> UUID -> IO ()
releasePollClaim pool token batchUuid = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUuid, token) Es.clearPollClaimBatchStmt
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()
