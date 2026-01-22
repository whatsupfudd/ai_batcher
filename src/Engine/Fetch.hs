{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Engine.Fetch where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, link, waitAnyCancel)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, void, when)

import qualified Data.ByteString.Lazy as BL
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)

import GHC.Generics (Generic)

import Data.Aeson (Value, (.=))
import qualified Data.Aeson as Aeson

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS
import Hasql.Statement (Statement)
import Hasql.TH

import qualified DB.EngineStmt as Es
import qualified Engine.Runner as R


--------------------------------------------------------------------------------
-- Public API

-- | Provider fetch error (align this to your Service.AiSystem error code).
data FetchError = FetchError
  { codeFE    :: Text
  , messageFE :: Text
  }
  deriving (Show, Eq, Generic)

newtype FetchEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

-- | Env wires DB + S3 + provider + next-stage queue.
--   Plug Service.AiSystem.fetchBatch into fetchBatchCT.
data Context = Context
  { pgPoolCT        :: Pool.Pool
  , nodeIdCT        :: Text
  , s3RepoCT         :: BL.ByteString -> IO UUID
  -- ^ Store object in S3 under UUID locator; return locator.
  , fetchBatchCT    :: UUID -> IO (Either FetchError Value)
  -- ^ Service.AiSystem.fetchBatch
  , enqueueGenDocCT :: Maybe (UUID -> IO ())
  -- ^ Send request_id to Engine.GenDoc main loop (next handler).
  }

data FetchConfig = FetchConfig
  { pollOutboxIntervalMicrosFC :: Int   -- how often to scan durable outbox
  , maxBatchesPerTickFC        :: Int   -- claim up to this many outbox rows per tick
  , queueDepthFC               :: Int   -- bounded in-memory queue depth
  , workerCountFC              :: Int   -- number of fetch workers
  , claimTtlSecondsFC          :: Int32 -- lease duration for outbox claims
  , errorBackoffSecondsFC      :: Int32 -- if provider fetch fails, delay retries by this many seconds
  }
  deriving (Show, Eq, Generic)

data FetchHandle = FetchHandle
  { asyncFH   :: Async ()
  , enqueueFH :: UUID -> IO ()
  -- ^ Enqueue a batch UUID (fast path from Engine.Poll).
  }


startFetchEngine :: Context -> FetchConfig -> IO FetchHandle
startFetchEngine ctxt cfg = do
  h <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthFC
    , workerCountES = cfg.workerCountFC
    , feedersES     = [outboxClaimerLoop ctxt cfg]
    , workerES      = workerLoop ctxt cfg
    }
  pure FetchHandle
    { asyncFH   = h.asyncEH
    , enqueueFH = \batchUuid -> h.enqueueEH (FetchJob batchUuid Nothing)
    }



{-- | Start Engine.Fetch with an internal queue.
--   Returns a handle including an enqueue function for Engine.Poll to call.
startFetchEngine :: Context -> FetchConfig -> IO FetchHandle
startFetchEngine env cfg = do
  q <- newTBQueueIO (fromIntegral cfg.queueDepthFC)
  a <- async (runFetchEngineWithQueue env cfg q)
  pure FetchHandle { asyncFH = a
        , enqueueFH = \batchUuid -> atomically (writeTBQueue q (FetchJob batchUuid Nothing))
    }

-- | Run using a provided queue (if you want to manage it elsewhere).
runFetchEngineWithQueue :: Context -> FetchConfig -> TBQueue FetchJob -> IO ()
runFetchEngineWithQueue ctxt cfg q = do
  outboxClaimerA <- async (outboxClaimerLoop ctxt cfg q)
  workersA <- mapM (\i -> async (workerLoop ctxt cfg i q)) [1 .. cfg.workerCountFC]

  link outboxClaimerA
  mapM_ link workersA

  void $ waitAnyCancel (outboxClaimerA : workersA)
-}

--------------------------------------------------------------------------------
-- Jobs

data FetchJob = FetchJob
  { batchUuidFJ  :: UUID
  , claimTokenFJ :: Maybe UUID
  }
  deriving (Show, Eq, Generic)

--------------------------------------------------------------------------------
-- Durable outbox claimer

outboxClaimerLoop :: Context -> FetchConfig -> TBQueue FetchJob -> IO ()
outboxClaimerLoop ctxt cfg q = forever $ do
  token <- nextRandom
  batches <- claimFetchOutboxMany ctxt.pgPoolCT ctxt.nodeIdCT token cfg.maxBatchesPerTickFC cfg.claimTtlSecondsFC
  if null batches
    then threadDelay cfg.pollOutboxIntervalMicrosFC
    else atomically $ mapM_ (\b -> writeTBQueue q (FetchJob b (Just token))) batches

claimFetchOutboxMany :: Pool.Pool -> Text -> UUID
            -> Int -> Int32 -> IO (Vector UUID)
claimFetchOutboxMany pool nodeId token limitN ttlSec = do
  eiRez <- Es.execStmt pool $
          Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) Es.claimFetchOutboxManyStmt
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector


-- For fast-path jobs that were enqueued by Engine.Poll (no claim token yet):
claimFetchOutboxOne
  :: Pool.Pool
  -> Text
  -> UUID
  -> UUID
  -> Int32
  -> IO Bool
claimFetchOutboxOne pool nodeId token batchUuid ttlSec = do
  eiRez <- Es.execStmt pool $
          Tx.statement (batchUuid, nodeId, token, ttlSec) Es.claimFetchOutboxOneStmt
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure (not (null aVector))


--------------------------------------------------------------------------------
-- Worker

workerLoop :: Context -> FetchConfig -> Int -> FetchJob -> IO ()
workerLoop ctxt cfg _workerIx job = forever $ do
  -- job <- atomically (readTBQueue q)

  -- Ensure we own a durable outbox claim for this batch (single-worker guarantee).
  (token, claimed) <- case job.claimTokenFJ of
    Just t  -> pure (t, True)
    Nothing -> do
      t <- nextRandom
      ok <- claimFetchOutboxOne ctxt.pgPoolCT ctxt.nodeIdCT t job.batchUuidFJ cfg.claimTtlSecondsFC
      pure (t, ok)

  if not claimed then
    pure ()  -- already claimed/processed elsewhere (or outbox row missing)
  else do
    res <- ctxt.fetchBatchCT job.batchUuidFJ
    case res of
      Left err -> do
        -- Log failure against requests (best-effort) and release claim with backoff.
        noteFetchFailed ctxt.pgPoolCT job.batchUuidFJ err
        releaseFetchOutboxClaim ctxt.pgPoolCT token job.batchUuidFJ cfg.errorBackoffSecondsFC

      Right rawJson -> do
        let bytes = Aeson.encode rawJson
            sz :: Int64
            sz = BL.length bytes

        loc <- ctxt.s3RepoCT bytes

        -- Persist S3 metadata + attach locator to requests + emit events + delete outbox (token protected).
        reqIds <- persistRawAndAttach ctxt.pgPoolCT token job.batchUuidFJ loc sz

        -- Push per-request downstream.
        case ctxt.enqueueGenDocCT of
          Just enqueueGDoc -> mapM_ enqueueGDoc reqIds
          Nothing -> pure ()


--------------------------------------------------------------------------------
-- DB serialization / updates

persistRawAndAttach :: Pool.Pool -> UUID -> UUID -> UUID -> Int64 -> IO (Vector UUID)
persistRawAndAttach pool token batchUuid rawLoc bytesSz = do
  eiRez <- Es.execStmt pool $ do
    Tx.statement (rawLoc, "raw_result" :: Text, bytesSz, "application/json" :: Text) Es.insertS3ObjectStmt

    -- attach locator to completed requests for this batch (idempotent if already set)
    reqIds <- Tx.statement (batchUuid, rawLoc) Es.attachRawLocatorStmt

    -- event per request
    mapM_ (\rid ->
          Tx.statement (rid, "completed" :: Text, rawFetchedDetails batchUuid rawLoc)
            Es.insertRequestEventStmt
      ) reqIds

    -- delete outbox row only if our token matches (prevents double-delete races)
    Tx.statement (batchUuid, token) Es.deleteFetchOutboxStmt
    pure reqIds

  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector


rawFetchedDetails :: UUID -> UUID -> Value
rawFetchedDetails batchUuid rawLoc = Aeson.object [ 
    "event" .= ("raw_fetched" :: Text)
  , "provider_batch_uuid" .= batchUuid
  , "raw_result_locator"  .= rawLoc
  ]

releaseFetchOutboxClaim :: Pool.Pool -> UUID -> UUID -> Int32 -> IO ()
releaseFetchOutboxClaim pool token batchUuid backoffSec = do
  eiRez <- Es.execStmt pool $
          Tx.statement (batchUuid, token, backoffSec) Es.releaseFetchOutboxClaimStmt
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()

--------------------------------------------------------------------------------
-- Failure logging (best-effort)

noteFetchFailed :: Pool.Pool -> UUID -> FetchError -> IO ()
noteFetchFailed pool batchUuid fetchErr = do
  eiRez <- Es.execStmt pool $ do
      reqIds <- Tx.statement batchUuid Es.listRequestsForBatchStmt
      let
        details = Aeson.object [ 
                "event" .= ("fetch_failed" :: Text)
              , "provider_batch_uuid" .= batchUuid
              , "error_code" .= fetchErr.codeFE
              , "error_message" .= fetchErr.messageFE
              ]
      mapM_ (\rid -> Tx.statement (rid, "completed" :: Text, details) Es.insertRequestEventStmt) reqIds
  case eiRez of
    Left _  -> pure () -- don't crash engine for logging failures
    Right _ -> pure ()
