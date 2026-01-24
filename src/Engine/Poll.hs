{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Engine.Poll
  ( Context(..)
  , PollConfig(..)
  , PollError(..)
  , PollHandle(..)
  , PollJob(..)
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
import Service.Types (ProviderBatchStatus(..)) -- expects: BatchRunning | BatchCompleted | BatchCancelled Text | BatchFailed Text

--------------------------------------------------------------------------------
-- Public API

data PollError = PollError
  { codePE    :: Text
  , messagePE :: Text
  }
  deriving (Show, Eq, Generic)

newtype PollEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

data Context = Context
  { pgPoolCT       :: Pool.Pool
  , nodeIdCT       :: Text
  , pollStatusCT   :: (UUID, Text) -> IO (Either PollError ProviderBatchStatus)
  , enqueueFetchCT :: (UUID, Text) -> IO () -- Fast path to Engine.Fetch (batch uid)
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
  , enqueuePH :: UUID -> Text -> IO ()
  }

data PollJob = PollJob
  { batchUidPJ   :: UUID
  , providerBatchIdPJ :: Text
  , claimTokenPJ :: Maybe UUID
  }
  deriving (Show, Eq, Generic)

startPollEngine :: Context -> PollConfig -> IO PollHandle
startPollEngine ctxt cfg = do
  engineHandle <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthPC
    , workerCountES = cfg.workerCountPC
    , feedersES     = [claimerFeeder ctxt cfg]
    , workerES      = \_i job -> pollWorker ctxt cfg job
    }
  pure PollHandle
    { asyncPH   = engineHandle.asyncEH
    , enqueuePH = \batchUid providerBatchId -> engineHandle.enqueueEH (PollJob batchUid providerBatchId Nothing)
    }

--------------------------------------------------------------------------------
-- Claimer (claims batches, not requests)

claimerFeeder :: Context -> PollConfig -> R.Feeder PollJob
claimerFeeder ctxt cfg q = forever $ do
  -- putStrLn "@[Poll.claimerFeeder] looping..."
  token <- nextRandom
  batches <- claimPollBatches ctxt.pgPoolCT ctxt.nodeIdCT token cfg.maxBatchesPerTickPC cfg.claimTtlSecondsPC
  -- putStrLn $ "@[Poll.claimerFeeder] batches: " <> show batches <> ", token: " <> show token
  if V.null batches then do
    -- putStrLn $ "@[Poll.claimerFeeder] no batches, sleeping for " <> show cfg.pollIntervalMicrosPC <> " microseconds, token: " <> show token
    threadDelay cfg.pollIntervalMicrosPC
    -- putStrLn $ "@[Poll.claimerFeeder] slept for " <> show cfg.pollIntervalMicrosPC <> " microseconds"
  else
    atomically $
      V.forM_ batches (\(batchUid, providerBatchId) -> writeTBQueue q (PollJob batchUid providerBatchId (Just token)))
  -- putStrLn $ "@[Poll.claimerFeeder] loop done (" <> show token <> ")"


claimPollBatches :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector (UUID, Text))
claimPollBatches pool nodeId token limitN ttlSec = do
  eiRez <- Es.execStmt pool $
      Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) Es.claimPollBatchesStmt
  case eiRez of
    Left err -> 
      do
        putStrLn $ "@[claimPollBatches] error: " <> show err
        throwIO (DbError (T.pack (show err)))
    Right aVector  -> pure aVector


-- Fast-path claim-one for a specific batch (when enqueued by Engine.Submit)
claimPollBatchOne :: Pool.Pool -> Text -> UUID -> UUID -> Int32 -> IO Bool
claimPollBatchOne pool nodeId token batchUid ttlSec = do
  putStrLn $ "@[claimPollBatchOne] batchUid: " <> show batchUid
  ei <- Es.execStmt pool $
      Tx.statement (batchUid, nodeId, token, ttlSec) Es.claimPollBatchOneStmt
  case ei of
    Left err -> do
      putStrLn $ "@[claimPollBatchOne] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right v  -> pure (not (V.null v))

--------------------------------------------------------------------------------
-- Worker

pollWorker :: Context -> PollConfig -> PollJob -> IO ()
pollWorker ctxt cfg job = do
  putStrLn $ "@[pollWorker] job: " <> show job
  (token, claimed) <- case job.claimTokenPJ of
    Just t  -> pure (t, True)
    Nothing -> do
      t  <- nextRandom
      ok <- claimPollBatchOne ctxt.pgPoolCT ctxt.nodeIdCT t job.batchUidPJ cfg.claimTtlSecondsPC
      pure (t, ok)

  putStrLn $ "@[pollWorker] claimed: " <> show claimed <> " token: " <> show token

  unless claimed (pure ())

  when claimed $ do
    -- Poll provider
    st <- ctxt.pollStatusCT (job.batchUidPJ, job.providerBatchIdPJ)

    case st of
      Left err -> do
        -- Track provider error (append-only) and release claim early for retry
        insertBatchEvent ctxt.pgPoolCT job.batchUidPJ "poll_error" (pollErrorDetails err.codePE err.messagePE)
        clearPollClaim ctxt.pgPoolCT job.batchUidPJ token

      Right status -> do
        -- Always record the fact we polled + the observed status (append-only)
        insertBatchEvent ctxt.pgPoolCT job.batchUidPJ "polled" (polledDetails status)

        case status of
          BatchRunning ->
            -- keep TTL claim; no further action
            pure ()
          
          BatchFinalizing -> do
            -- keep TTL claim; no further action
            pure ()
          
          BatchInProgress -> do
            -- keep TTL claim; no further action
            pure ()
          
          BatchValidating -> do
            -- keep TTL claim; no further action
            pure ()
          
          BatchCompleted -> do
            insertBatchEvent ctxt.pgPoolCT job.batchUidPJ "completed" (terminalDetails "completed" Nothing)
            insertFetchOutbox ctxt.pgPoolCT job.batchUidPJ
            clearPollClaim ctxt.pgPoolCT job.batchUidPJ token
            ctxt.enqueueFetchCT (job.batchUidPJ, job.providerBatchIdPJ)

          BatchCancelled reason -> do
            insertBatchEvent ctxt.pgPoolCT job.batchUidPJ "cancelled" (terminalDetails "cancelled" (Just reason))
            reqIds <- markBatchRequestsCancelled ctxt.pgPoolCT job.batchUidPJ token
            insertCancelledRequestEvents ctxt.pgPoolCT job.batchUidPJ "cancelled" reason reqIds
            clearPollClaim ctxt.pgPoolCT job.batchUidPJ token

          BatchFailed reason -> do
            insertBatchEvent ctxt.pgPoolCT job.batchUidPJ "failed" (terminalDetails "failed" (Just reason))
            reqIds <- markBatchRequestsCancelled ctxt.pgPoolCT job.batchUidPJ token
            insertCancelledRequestEvents ctxt.pgPoolCT job.batchUidPJ "failed" reason reqIds
            clearPollClaim ctxt.pgPoolCT job.batchUidPJ token

--------------------------------------------------------------------------------
-- DB ops

insertBatchEvent :: Pool.Pool -> UUID -> Text -> Value -> IO ()
insertBatchEvent pool batchUid eventType details = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, eventType, details) Es.insertBatchEventStmt
  case ei of
    Left err -> do
      putStrLn $ "@[insertBatchEvent] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

insertFetchOutbox :: Pool.Pool -> UUID -> IO ()
insertFetchOutbox pool batchUid = do
  ei <- Es.execStmt pool $
          Tx.statement batchUid Es.insertFetchOutboxStmt
  case ei of
    Left err -> do
      putStrLn $ "@[insertFetchOutbox] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

markBatchRequestsCancelled :: Pool.Pool -> UUID -> UUID -> IO (Vector UUID)
markBatchRequestsCancelled pool batchUid pollToken = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, pollToken) Es.markBatchRequestsCancelledStmt
  case ei of
    Left err -> do
      putStrLn $ "@[markBatchRequestsCancelled] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right v  -> pure v

-- Append request_events for cancelled/failed (append-only history).
insertCancelledRequestEvents :: Pool.Pool -> UUID -> Text -> Text -> Vector UUID -> IO ()
insertCancelledRequestEvents pool batchUid kind reason reqIds = do
  let details = requestTerminalDetails batchUid kind reason
  ei <- Es.execStmt pool $ do
          V.forM_ reqIds (\rid ->
            Tx.statement (rid, "cancelled" :: Text, details) Es.insertRequestEventStmt
            )
  case ei of
    Left err -> do
      putStrLn $ "@[insertCancelledRequestEvents] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()


clearPollClaim :: Pool.Pool -> UUID -> UUID -> IO ()
clearPollClaim pool batchUid token = do
  putStrLn $ "@[clearPollClaim] batchUid: " <> show batchUid <> ", token: " <> show token
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, token) Es.clearPollClaimBatchStmt
  case ei of
    Left err -> do
      putStrLn $ "@[clearPollClaim] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()


--------------------------------------------------------------------------------
-- Event details

polledDetails :: ProviderBatchStatus -> Value
polledDetails st =
  Ae.object
    [ "event" Ae..= ("polled" :: Text)
    , "provider_status" Ae..= providerStatusText st
    , "reason" Ae..= providerReason st
    ]

providerStatusText :: ProviderBatchStatus -> Text
providerStatusText st =
  case st of
    BatchRunning        -> "running"
    BatchCompleted      -> "completed"
    BatchCancelled _    -> "cancelled"
    BatchFailed _       -> "failed"

providerReason :: ProviderBatchStatus -> Value
providerReason st =
  case st of
    BatchCancelled r -> Ae.toJSON r
    BatchFailed r    -> Ae.toJSON r
    _                -> Ae.Null

terminalDetails :: Text -> Maybe Text -> Value
terminalDetails kind mReason =
  Ae.object $
    [ "event" Ae..= ("provider_terminal" :: Text)
    , "terminal_kind" Ae..= kind
    ] <> case mReason of
           Nothing -> []
           Just r  -> ["reason" Ae..= r]

pollErrorDetails :: Text -> Text -> Value
pollErrorDetails code msg =
  Ae.object
    [ "event" Ae..= ("poll_error" :: Text)
    , "error_code" Ae..= code
    , "error_message" Ae..= msg
    ]

requestTerminalDetails :: UUID -> Text -> Text -> Value
requestTerminalDetails batchUid kind reason =
  Ae.object
    [ "event" Ae..= ("batch_terminal" :: Text)
    , "batch_uid" Ae..= batchUid
    , "terminal_kind" Ae..= kind
    , "reason" Ae..= reason
    ]
