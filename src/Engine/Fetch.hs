{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}

module Engine.Fetch where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, link, waitAnyCancel)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, void, when)
import Data.Aeson (Value, (.=))
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)

import GHC.Generics (Generic)

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS
import Hasql.Statement (Statement)
import Hasql.TH

--------------------------------------------------------------------------------
-- Public API

-- | Provider fetch error (align this to your Service.AiSystem error code).
data FetchError = FetchError
  { feCode    :: Text
  , feMessage :: Text
  }
  deriving (Show, Eq, Generic)

newtype FetchEngineError
  = DbError Text
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
  , enqueueGenDocCT :: UUID -> IO ()
  -- ^ Send request_id to Engine.GenDoc main loop (next handler).
  }

data FetchConfig = FetchConfig
  { fcPollOutboxIntervalMicros :: Int   -- how often to scan durable outbox
  , fcMaxBatchesPerTick        :: Int   -- claim up to this many outbox rows per tick
  , fcQueueDepth               :: Int   -- bounded in-memory queue depth
  , fcWorkerCount              :: Int   -- number of fetch workers
  , fcClaimTtlSeconds          :: Int32 -- lease duration for outbox claims
  , fcErrorBackoffSeconds      :: Int32 -- if provider fetch fails, delay retries by this many seconds
  }
  deriving (Show, Eq, Generic)

data FetchHandle = FetchHandle
  { fhAsync   :: Async ()
  , fhEnqueue :: UUID -> IO ()
  -- ^ Enqueue a batch UUID (fast path from Engine.Poll).
  }

-- | Start Engine.Fetch with an internal queue.
--   Returns a handle including an enqueue function for Engine.Poll to call.
startFetchEngine :: Context -> FetchConfig -> IO FetchHandle
startFetchEngine env cfg@FetchConfig{..} = do
  q <- newTBQueueIO (fromIntegral fcQueueDepth)
  a <- async (runFetchEngineWithQueue env cfg q)
  let enqueueFn batchUuid = atomically (writeTBQueue q (FetchJob batchUuid Nothing))
  pure FetchHandle { fhAsync = a, fhEnqueue = enqueueFn }

-- | Run using a provided queue (if you want to manage it elsewhere).
runFetchEngineWithQueue :: Context -> FetchConfig -> TBQueue FetchJob -> IO ()
runFetchEngineWithQueue env@Context{..} cfg@FetchConfig{..} q = do
  outboxClaimerA <- async (outboxClaimerLoop env cfg q)
  workersA <- mapM (\i -> async (workerLoop env cfg i q)) [1 .. fcWorkerCount]

  link outboxClaimerA
  mapM_ link workersA

  void $ waitAnyCancel (outboxClaimerA : workersA)

--------------------------------------------------------------------------------
-- Jobs

data FetchJob = FetchJob
  { fjBatchUuid  :: UUID
  , fjClaimToken :: Maybe UUID
  }
  deriving (Show, Eq, Generic)

--------------------------------------------------------------------------------
-- Durable outbox claimer

outboxClaimerLoop :: Context -> FetchConfig -> TBQueue FetchJob -> IO ()
outboxClaimerLoop Context{..} FetchConfig{..} q = forever $ do
  token <- nextRandom
  batches <- claimFetchOutboxMany pgPoolCT nodeIdCT token fcMaxBatchesPerTick fcClaimTtlSeconds
  if null batches
    then threadDelay fcPollOutboxIntervalMicros
    else atomically $ mapM_ (\b -> writeTBQueue q (FetchJob b (Just token))) batches

claimFetchOutboxMany :: Pool.Pool -> Text -> UUID
            -> Int -> Int32 -> IO (Vector UUID)
claimFetchOutboxMany pool nodeId token limitN ttlSec = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
          Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) claimFetchOutboxManyStmt

  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector


claimFetchOutboxManyStmt :: Statement (Int32, Text, UUID, Int32) (Vector UUID)
claimFetchOutboxManyStmt =
  [vectorStatement|
    with picked as (
      select o.provider_batch_uuid
        from batcher.fetch_outbox o
       where (o.fetch_claimed_until is null or o.fetch_claimed_until < now())
       order by o.created_at asc
       limit $1 :: int4
       for update skip locked
    )
    update batcher.fetch_outbox o
       set fetch_claimed_by    = $2 :: text,
           fetch_claim_token   = $3 :: uuid,
           fetch_claimed_until = now() + make_interval(secs => $4 :: int4)
      from picked p
     where o.provider_batch_uuid = p.provider_batch_uuid
     returning o.provider_batch_uuid :: uuid
  |]

-- For fast-path jobs that were enqueued by Engine.Poll (no claim token yet):
claimFetchOutboxOne
  :: Pool.Pool
  -> Text
  -> UUID
  -> UUID
  -> Int32
  -> IO Bool
claimFetchOutboxOne pool nodeId token batchUuid ttlSec = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
            Tx.statement (batchUuid, nodeId, token, ttlSec) claimFetchOutboxOneStmt

  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure (not (null aVector))


claimFetchOutboxOneStmt :: Statement (UUID, Text, UUID, Int32) (Vector UUID)
claimFetchOutboxOneStmt =
  [vectorStatement|
    update batcher.fetch_outbox
       set fetch_claimed_by    = $2 :: text,
           fetch_claim_token   = $3 :: uuid,
           fetch_claimed_until = now() + make_interval(secs => $4 :: int4)
     where provider_batch_uuid = $1 :: uuid
       and (fetch_claimed_until is null or fetch_claimed_until < now())
     returning provider_batch_uuid :: uuid
  |]

--------------------------------------------------------------------------------
-- Worker

workerLoop :: Context -> FetchConfig -> Int -> TBQueue FetchJob -> IO ()
workerLoop env@Context{..} FetchConfig{..} _workerIx q = forever $ do
  FetchJob{..} <- atomically (readTBQueue q)

  -- Ensure we own a durable outbox claim for this batch (single-worker guarantee).
  (token, claimed) <- case fjClaimToken of
    Just t  -> pure (t, True)
    Nothing -> do
      t <- nextRandom
      ok <- claimFetchOutboxOne pgPoolCT nodeIdCT t fjBatchUuid fcClaimTtlSeconds
      pure (t, ok)

  if not claimed
    then pure ()  -- already claimed/processed elsewhere (or outbox row missing)
    else do
      res <- fetchBatchCT fjBatchUuid
      case res of
        Left err -> do
          -- Log failure against requests (best-effort) and release claim with backoff.
          noteFetchFailed pgPoolCT fjBatchUuid err
          releaseFetchOutboxClaim pgPoolCT token fjBatchUuid fcErrorBackoffSeconds

        Right rawJson -> do
          let bytes = Aeson.encode rawJson
              sz :: Int64
              sz = BL.length bytes

          loc <- s3RepoCT bytes

          -- Persist S3 metadata + attach locator to requests + emit events + delete outbox (token protected).
          reqIds <- persistRawAndAttach pgPoolCT token fjBatchUuid loc sz

          -- Push per-request downstream.
          mapM_ enqueueGenDocCT reqIds

--------------------------------------------------------------------------------
-- DB serialization / updates

persistRawAndAttach :: Pool.Pool -> UUID -> UUID -> UUID -> Int64 -> IO (Vector UUID)
persistRawAndAttach pool token batchUuid rawLoc bytesSz = do
  let session =
        TxS.transaction TxS.ReadCommitted TxS.Write $ do
          -- record the raw result object in S3 metadata index
          Tx.statement (rawLoc, "raw_result" :: Text, bytesSz, "application/json" :: Text) insertS3ObjectStmt

          -- attach locator to completed requests for this batch (idempotent if already set)
          reqIds <- Tx.statement (batchUuid, rawLoc) attachRawLocatorStmt

          -- event per request
          mapM_ (\rid ->
                Tx.statement (rid, "completed" :: Text, rawFetchedDetails batchUuid rawLoc)
                  insertRequestEventStmt
            ) reqIds

          -- delete outbox row only if our token matches (prevents double-delete races)
          Tx.statement (batchUuid, token) deleteFetchOutboxStmt
          pure reqIds

  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right aVector -> pure aVector


rawFetchedDetails :: UUID -> UUID -> Value
rawFetchedDetails batchUuid rawLoc = Aeson.object [ 
    "event" .= ("raw_fetched" :: Text)
  , "provider_batch_uuid" .= batchUuid
  , "raw_result_locator"  .= rawLoc
  ]


-- Attach raw_result_locator to requests in that batch (only those completed).
-- We do "set if null" so repeat fetches are harmless.
attachRawLocatorStmt :: Statement (UUID, UUID) (Vector UUID)
attachRawLocatorStmt = [vectorStatement|
    update batcher.requests
       set raw_result_locator = coalesce(raw_result_locator, $2 :: uuid),
           updated_at         = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'completed'
     returning request_id :: uuid
  |]


insertS3ObjectStmt :: Statement (UUID, Text, Int64, Text) ()
insertS3ObjectStmt = [resultlessStatement|
    insert into batcher.s3_objects (locator, kind, bytes, content_type)
    values ($1 :: uuid, $2 :: text, $3 :: int8, $4 :: text)
    on conflict (locator) do nothing
  |]


insertRequestEventStmt :: Statement (UUID, Text, Value) ()
insertRequestEventStmt = [resultlessStatement|
    insert into batcher.request_events (request_id, state, details)
    values ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]


deleteFetchOutboxStmt :: Statement (UUID, UUID) ()
deleteFetchOutboxStmt = [resultlessStatement|
    delete from batcher.fetch_outbox
     where provider_batch_uuid = $1 :: uuid
       and fetch_claim_token   = $2 :: uuid
  |]

releaseFetchOutboxClaim :: Pool.Pool -> UUID -> UUID -> Int32 -> IO ()
releaseFetchOutboxClaim pool token batchUuid backoffSec = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $
          Tx.statement (batchUuid, token, backoffSec) releaseFetchOutboxClaimStmt
  eiRez <- Pool.use pool session
  case eiRez of
    Left err  -> throwIO (DbError (T.pack (show err)))
    Right _ -> pure ()

releaseFetchOutboxClaimStmt :: Statement (UUID, UUID, Int32) ()
releaseFetchOutboxClaimStmt = [resultlessStatement|
    update batcher.fetch_outbox
       set fetch_claimed_by    = null,
           fetch_claim_token   = null,
           fetch_claimed_until = now() + make_interval(secs => $3 :: int4)
     where provider_batch_uuid = $1 :: uuid
       and fetch_claim_token   = $2 :: uuid
  |]

--------------------------------------------------------------------------------
-- Failure logging (best-effort)

noteFetchFailed :: Pool.Pool -> UUID -> FetchError -> IO ()
noteFetchFailed pool batchUuid FetchError{..} = do
  let
    session = TxS.transaction TxS.ReadCommitted TxS.Write $ do
      reqIds <- Tx.statement batchUuid listRequestsForBatchStmt
      let
        details = Aeson.object [ 
                "event" .= ("fetch_failed" :: Text)
              , "provider_batch_uuid" .= batchUuid
              , "error_code" .= feCode
              , "error_message" .= feMessage
              ]
      mapM_ (\rid -> Tx.statement (rid, "completed" :: Text, details) insertRequestEventStmt) reqIds

  eiRez <- Pool.use pool session
  case eiRez of
    Left _  -> pure () -- don't crash engine for logging failures
    Right _ -> pure ()


listRequestsForBatchStmt :: Statement UUID (Vector UUID)
listRequestsForBatchStmt = [vectorStatement|
    select request_id :: uuid
      from batcher.requests
     where provider_batch_uuid = $1 :: uuid
       and state = 'completed'
  |]
