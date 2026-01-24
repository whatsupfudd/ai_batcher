{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Engine.Fetch
  ( Context(..)
  , FetchConfig(..)
  , FetchError(..)
  , FetchHandle(..)
  , startFetchEngine
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async)
import Control.Concurrent.STM
import Control.Exception (Exception, throwIO)
import Control.Monad (forever, unless, void, when)

import Data.Aeson (Value, (.:), (.:?), (.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as AT
import qualified Data.ByteString.Lazy as BL
import Data.Either (partitionEithers)
import Data.Int (Int32, Int64)
import Data.Maybe (mapMaybe, catMaybes, fromMaybe)
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)
import qualified Data.UUID as Uu
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)
import qualified Data.Vector as V

import GHC.Generics (Generic)

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx

import qualified DB.EngineStmt as Es
import qualified Engine.Runner as R
import qualified Service.Types as St

--------------------------------------------------------------------------------
-- Public API

data FetchError = FetchError
  { codeFE    :: Text
  , messageFE :: Text
  }
  deriving (Show, Eq, Generic)

newtype FetchEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

data Context = Context
  { pgPoolCT :: Pool.Pool
  , nodeIdCT :: Text
  , s3RepoCT :: BL.ByteString -> IO UUID
  -- ^ Store object in S3 under UUID locator; return locator.
  , fetchBatchCT :: (UUID, Text) -> IO (Either FetchError (BL.ByteString, Vector (Either String St.RequestResult)))
  -- ^ Service.AiSystem.fetchBatch : returns a big JSON for the whole batch
  , enqueueGenDocCT :: Maybe (UUID -> IO ())
  -- ^ Send request_id to next engine (optional; safe to no-op for now)
  }

data FetchConfig = FetchConfig
  { pollOutboxIntervalMicrosFC :: Int
  , maxBatchesPerTickFC        :: Int
  , queueDepthFC               :: Int
  , workerCountFC              :: Int
  , claimTtlSecondsFC          :: Int32
  , errorBackoffSecondsFC      :: Int32
  }
  deriving (Show, Eq, Generic)

data FetchHandle = FetchHandle
  { asyncFH   :: Async ()
  , enqueueFH :: (UUID, Text) -> IO ()
  -- ^ Enqueue a batch uid (fast path from Engine.Poll)
  }

--------------------------------------------------------------------------------
-- Engine spec + jobs

data FetchJob = FetchJob
  { batchUidFJ   :: UUID
  , providerBatchIdFJ :: Text
  , claimTokenFJ :: Maybe UUID
  }
  deriving (Show, Eq, Generic)

startFetchEngine :: Context -> FetchConfig -> IO FetchHandle
startFetchEngine ctxt cfg = do
  h <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthFC
    , workerCountES = cfg.workerCountFC
    , feedersES     = [outboxClaimerFeeder ctxt cfg]
    , workerES      = \_i job -> fetchWorker ctxt cfg job
    }
  pure FetchHandle
    { asyncFH   = h.asyncEH
    , enqueueFH = \(batchUid, providerBatchId) -> h.enqueueEH (FetchJob batchUid providerBatchId Nothing)
    }

--------------------------------------------------------------------------------
-- Durable outbox claimer

outboxClaimerFeeder :: Context -> FetchConfig -> R.Feeder FetchJob
outboxClaimerFeeder ctxt cfg q = forever $ do
  token <- nextRandom
  batches <- claimFetchOutboxMany ctxt.pgPoolCT ctxt.nodeIdCT token cfg.maxBatchesPerTickFC cfg.claimTtlSecondsFC
  if V.null batches then 
      threadDelay cfg.pollOutboxIntervalMicrosFC
  else 
    atomically $ V.forM_ batches (\(batchUid, providerBatchId) ->
              writeTBQueue q (FetchJob batchUid providerBatchId (Just token))
            )


claimFetchOutboxMany :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector (UUID, Text))
claimFetchOutboxMany pool nodeId token limitN ttlSec = do
  -- putStrLn $ "@[claimFetchOutboxOne] token: " <> show token
  eiRez <- Es.execStmt pool $
          Tx.statement (fromIntegral limitN, nodeId, token, ttlSec) Es.claimFetchOutboxManyStmt
  case eiRez of
    Left err -> do
      putStrLn $ "@[claimFetchOutboxMany] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right aVector  -> pure aVector


claimFetchOutboxOne :: Pool.Pool -> Text -> UUID -> UUID -> Int32 -> IO Bool
claimFetchOutboxOne pool nodeId token batchUid ttlSec = do
  -- putStrLn $ "@[claimFetchOutboxOne] batchUid: " <> show batchUid
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, nodeId, token, ttlSec) Es.claimFetchOutboxOneStmt
  case ei of
    Left err -> do
      putStrLn $ "@[claimFetchOutboxOne] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right v  -> pure (not (V.null v))

--------------------------------------------------------------------------------
-- Worker

fetchWorker :: Context -> FetchConfig -> FetchJob -> IO ()
fetchWorker ctxt cfg job = do
  putStrLn $ "@[fetchWorker] job: " <> show job
  (token, claimed) <- case job.claimTokenFJ of
    Just t  -> pure (t, True)
    Nothing -> do
      t  <- nextRandom
      ok <- claimFetchOutboxOne ctxt.pgPoolCT ctxt.nodeIdCT t job.batchUidFJ cfg.claimTtlSecondsFC
      pure (t, ok)
  putStrLn $ "@[fetchWorker] claimed: " <> show claimed <> " token: " <> show token
  unless claimed (pure ()) -- already claimed/processed elsewhere or outbox row missing

  when claimed $ do
    eiRez <- ctxt.fetchBatchCT (job.batchUidFJ, job.providerBatchIdFJ)
    case eiRez of
      Left err -> do
        -- record failure + backoff
        putStrLn $ "@[fetchWorker] fetch_failed: " <> show err
        insertBatchEvent ctxt.pgPoolCT job.batchUidFJ "fetch_failed" (fetchFailedDetails err.codeFE err.messageFE)
        releaseFetchOutboxClaim ctxt.pgPoolCT job.batchUidFJ token cfg.errorBackoffSecondsFC

      Right (rawJson, decodedResults) ->
        let
          listResult = V.toList decodedResults
          (badResults, goodResults) = partitionEithers listResult
          sz = BL.length rawJson
        in do
        
        -- Store raw JSON in S3
        rawLoc <- ctxt.s3RepoCT rawJson
        -- putStrLn $ "@[fetchWorker] result: " <> show rawJson
        -- putStrLn $ "@[fetchWorker] s3Loc: " <> show rawLoc
        persistRawS3AndEvent ctxt.pgPoolCT job.batchUidFJ rawLoc sz

        -- Load requests for this batch
        reqIdsVec <- listBatchRequests ctxt.pgPoolCT job.batchUidFJ
        let reqSet :: Set UUID
            reqSet = S.fromList (V.toList reqIdsVec)


        -- Materialize per-request answers + request completion
        (okCount, missCount) <- persistAnswers ctxt.pgPoolCT job.batchUidFJ goodResults

        -- Batch progression
        insertBatchEvent ctxt.pgPoolCT job.batchUidFJ "answers_materialized"
            (answersMaterializedDetails okCount missCount (V.length reqIdsVec))
        putStrLn $ "@[fetchWorker] answers_materialized: " <> show (answersMaterializedDetails okCount missCount (V.length reqIdsVec))
        -- remove outbox row (token protected)
        deleteFetchOutbox ctxt.pgPoolCT job.batchUidFJ token

--------------------------------------------------------------------------------
-- DB ops

persistRawS3AndEvent :: Pool.Pool -> UUID -> UUID -> Int64 -> IO ()
persistRawS3AndEvent pool batchUid rawLoc bytesSz = do
  let details = rawFetchedDetails batchUid rawLoc bytesSz
  ei <- Es.execStmt pool $ do
          Tx.statement (rawLoc, "raw_result" :: Text, bytesSz, "application/json" :: Text) Es.insertS3ObjectStmt
          Tx.statement (batchUid, "raw_fetched" :: Text, details) Es.insertBatchEventStmt
  case ei of
    Left err -> do
      putStrLn $ "@[persistRawS3AndEvent] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

listBatchRequests :: Pool.Pool -> UUID -> IO (Vector UUID)
listBatchRequests pool batchUid = do
  ei <- Es.execStmt pool $
          Tx.statement batchUid Es.listBatchRequestsStmt
  case ei of
    Left err -> do
      putStrLn $ "@[listBatchRequests] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right v  -> pure v

persistAnswers :: Pool.Pool -> UUID -> [St.RequestResult] -> IO (Int, Int)
persistAnswers pool batchUid answers = do
  -- okCount = inserted/updated answers, missCount = 0 here (miss computed earlier)
  let okCount = length answers
      missCount = 0
  ei <- Es.execStmt pool $ do
          V.forM_ (V.fromList answers) $ \rResult ->
            let
              requestId = rResult.requestId
              metaData = fromMaybe Aeson.Null rResult.metaData
              content = fromMaybe "" rResult.content
            in do
            Tx.statement (batchUid, requestId, metaData, content) Es.upsertRequestResultStmt
            Tx.statement (requestId, "completed" :: Text, requestCompletedDetails batchUid metaData) Es.insertRequestEventStmt
            Tx.statement requestId Es.markRequestCompletedStmt
  case ei of
    Left err -> do
      putStrLn $ "@[persistAnswers] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure (okCount, missCount)

insertBatchEvent :: Pool.Pool -> UUID -> Text -> Value -> IO ()
insertBatchEvent pool batchUid eventType details = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, eventType, details) Es.insertBatchEventStmt
  case ei of
    Left err -> do
      putStrLn $ "@[insertBatchEvent] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

deleteFetchOutbox :: Pool.Pool -> UUID -> UUID -> IO ()
deleteFetchOutbox pool batchUid token = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, token) Es.deleteFetchOutboxStmt
  case ei of
    Left err -> do
      putStrLn $ "@[deleteFetchOutbox] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

releaseFetchOutboxClaim :: Pool.Pool -> UUID -> UUID -> Int32 -> IO ()
releaseFetchOutboxClaim pool batchUid token backoffSec = do
  ei <- Es.execStmt pool $
          Tx.statement (batchUid, token, backoffSec) Es.releaseFetchOutboxClaimStmt
  case ei of
    Left err -> do
      putStrLn $ "@[releaseFetchOutboxClaim] error: " <> show err
      throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()

--------------------------------------------------------------------------------
-- JSON extraction (provider output -> (request_id, metadata, content))

-- | Extract answers from a provider batch JSON using custom_id.
--   We accept multiple plausible shapes:
--     1) {"results":[{"custom_id":"..","output_text":"..","metadata":{..}}, ...]}
--     2) {"data":[{"custom_id":"..","response":{...}}, ...]}
--     3) OpenAI-ish JSONL-turned-JSON: {"items":[{"custom_id":"..","response":{"output":[...]}}, ...]}
--
--   For robust production use, adjust this parser to match your actual fetchBatch payload.
extractAnswers :: Value -> Set UUID -> [(UUID, Value, Text)]
extractAnswers v allowed =
  case AT.parseMaybe parseTop v of
    Nothing   -> []
    Just xs   -> filter (\(rid, _, _) -> rid `S.member` allowed) xs
  where
    parseTop :: Value -> AT.Parser [(UUID, Value, Text)]
    parseTop =
      Aeson.withObject "batchResult" $ \o -> do
        mResults <- o .:? "results"
        mData    <- o .:? "data"
        mItems   <- o .:? "items"
        case (mResults, mData, mItems) of
          (Just (Aeson.Array arr), _, _) -> parseArray parseResultObj arr
          (_, Just (Aeson.Array arr), _) -> parseArray parseDataObj arr
          (_, _, Just (Aeson.Array arr)) -> parseArray parseItemObj arr
          _ -> pure []

    parseArray :: (Value -> AT.Parser (Maybe (UUID, Value, Text))) -> Aeson.Array -> AT.Parser [(UUID, Value, Text)]
    parseArray p arr = do
      ms <- traverse p (V.toList arr)
      pure $ catMaybes ms

    parseUuidText :: Text -> Maybe UUID
    parseUuidText t = Uu.fromText t

    parseResultObj :: Value -> AT.Parser (Maybe (UUID, Value, Text))
    parseResultObj =
      Aeson.withObject "result" $ \o -> do
        cid <- o .: "custom_id"
        let mRid = parseUuidText cid
        case mRid of
          Nothing -> pure Nothing
          Just rid -> do
            meta <- o .:? "metadata" >>= \mm -> pure (maybe (Aeson.object []) id mm)
            -- prefer output_text, fall back to content, fall back to pretty json
            mTxt <- o .:? "output_text"
            mTxt2 <- o .:? "content"
            let txt = maybe (maybe "" id mTxt2) id mTxt
            pure (Just (rid, meta, txt))

    parseDataObj :: Value -> AT.Parser (Maybe (UUID, Value, Text))
    parseDataObj =
      Aeson.withObject "dataItem" $ \o -> do
        cid <- o .: "custom_id"
        let mRid = parseUuidText cid
        case mRid of
          Nothing -> pure Nothing
          Just rid -> do
            -- keep whole object as metadata by default
            let meta = Aeson.Object o
            -- try common field names
            mTxt <- o .:? "output_text"
            mTxt2 <- o .:? "text"
            let txt = maybe (maybe "" id mTxt2) id mTxt
            pure (Just (rid, meta, txt))

    parseItemObj :: Value -> AT.Parser (Maybe (UUID, Value, Text))
    parseItemObj =
      Aeson.withObject "item" $ \o -> do
        cid <- o .: "custom_id"
        let mRid = parseUuidText cid
        case mRid of
          Nothing -> pure Nothing
          Just rid -> do
            mResp <- o .:? "response"
            let meta = Aeson.Object o
            txt <- case mResp of
              Just rv -> pure (extractTextFromResponse rv)
              Nothing -> pure ""
            pure (Just (rid, meta, txt))

-- Extremely permissive: try to find text in common response shapes.
extractTextFromResponse :: Value -> Text
extractTextFromResponse v =
  case AT.parseMaybe parseOne v of
    Just t  -> t
    Nothing -> ""
  where
    parseOne :: Value -> AT.Parser Text
    parseOne =
      Aeson.withObject "resp" $ \o -> do
        -- try {output_text:"..."}
        mA <- o .:? "output_text"
        case mA of
          Just t -> pure t
          Nothing -> do
            -- try {text:"..."}
            mB <- o .:? "text"
            case mB of
              Just t -> pure t
              Nothing -> pure ""

--------------------------------------------------------------------------------
-- Event details

rawFetchedDetails :: UUID -> UUID -> Int64 -> Value
rawFetchedDetails batchUid rawLoc bytesSz =
  Aeson.object
    [ "event" .= ("raw_fetched" :: Text)
    , "batch_uid" .= batchUid
    , "raw_result_locator" .= rawLoc
    , "bytes" .= bytesSz
    ]

requestCompletedDetails :: UUID -> Value -> Value
requestCompletedDetails batchUid meta =
  Aeson.object
    [ "event" .= ("answer_materialized" :: Text)
    , "batch_uid" .= batchUid
    , "metadata" .= meta
    ]

answersMaterializedDetails :: Int -> Int -> Int -> Value
answersMaterializedDetails okCount missCount totalReqs =
  Aeson.object
    [ "event" .= ("answers_materialized" :: Text)
    , "ok_count" .= okCount
    , "missing_count" .= missCount
    , "batch_request_count" .= totalReqs
    ]

fetchFailedDetails :: Text -> Text -> Value
fetchFailedDetails code msg =
  Aeson.object
    [ "event" .= ("fetch_failed" :: Text)
    , "error_code" .= code
    , "error_message" .= msg
    ]
