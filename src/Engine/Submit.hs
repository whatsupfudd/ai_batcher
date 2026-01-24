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
import Control.Monad (forever, when)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae
import Data.Int (Int32)
import qualified Data.List as L
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)
import Data.UUID as Uu
import Data.UUID.V4 (nextRandom)
import Data.Vector (Vector)
import qualified Data.Vector as V

import GHC.Generics (Generic)

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx

import qualified DB.EngineStmt as Es
import qualified Engine.Runner as R
import qualified Engine.Support as Sup

data Context = Context
  { pgPoolCT       :: Pool.Pool
  , nodeIdCT       :: Text
  , sendRequestCT  :: NonEmpty (UUID, Text) -> IO (Either SubmitError SubmitOk)
  , enqueuePollCT  :: UUID -> IO ()         -- FAST PATH to Engine.Poll
  }

data SubmitConfig = SubmitConfig
  { pollIntervalMicrosSC :: Int
  , batchSizeSC          :: Int
  , queueDepthSC         :: Int
  , workerCountSC        :: Int
  , claimTtlSecondsSC    :: Int32
  }
  deriving (Show, Eq, Generic)

data SubmitOk = SubmitOk { 
    providerIDSO :: Text
  , batchIDSO :: UUID
  }
  deriving (Show, Eq, Generic)

data SubmitError = SubmitError { 
    codeSE :: Text
  , messageSE :: Text
  }
  deriving (Show, Eq, Generic)

newtype SubmitEngineError = DbError Text
  deriving (Show, Eq, Generic, Exception)

data SubmitHandle = SubmitHandle
  { asyncSH :: Async ()
  , enqueueSH :: SubmitMsg -> IO ()
  }

data SubmitMsg
  = SubmitKick
  deriving (Show, Eq, Generic)

data ClaimedRequest = ClaimedRequest
  { requestIDCR   :: UUID
  , requestTextCR :: Text
  }
  deriving (Show, Eq, Generic)


startSubmitEngine :: Context -> SubmitConfig -> IO SubmitHandle
startSubmitEngine ctxt cfg = do
  h <- R.startEngine R.EngineSpec
    { queueDepthES  = cfg.queueDepthSC
    , workerCountES = cfg.workerCountSC
    , feedersES     = [kickFeeder cfg]
    , workerES      = \_i msg -> submitWorker ctxt cfg msg
    }
  pure SubmitHandle
    { asyncSH = h.asyncEH
    , enqueueSH = h.enqueueEH
    }


kickFeeder :: SubmitConfig -> R.Feeder SubmitMsg
kickFeeder cfg q = forever $ do
  threadDelay cfg.pollIntervalMicrosSC
  -- avoid building up infinite kicks if queue is full: tryWrite
  atomically $ do
    _ <- Sup.tryWriteTBQueue q SubmitKick
    pure ()


submitWorker :: Context -> SubmitConfig -> SubmitMsg -> IO ()
submitWorker ctxt cfg SubmitKick = drainOnce
  where
  drainOnce = do
    putStrLn "@[submitWorker] draining...."
    batchUuid <- nextRandom
    claimed <- claimEnteredRequests ctxt.pgPoolCT ctxt.nodeIdCT batchUuid cfg.batchSizeSC cfg.claimTtlSecondsSC
    if V.null claimed then do
      putStrLn "@[submitWorker] no claims."
      pure ()
    else do
      putStrLn $ "@[submitWorker] claims: " <> L.intercalate "\n" (V.toList $ V.map show claimed)
      processOne batchUuid claimed
      drainOnce

  processOne batchUuid claimedVec = do
    putStrLn "@[submitWorker] processing..."
    let
      claimList = V.toList claimedVec
      reqPairs  = NE.map (\cr -> (cr.requestIDCR, cr.requestTextCR)) $ NE.fromList claimList

    putStrLn $ "@[submitWorker] sending request: " <> show reqPairs
    res <- ctxt.sendRequestCT reqPairs
    putStrLn $ "@[submitWorker] response: " <> show res
    case res of
      Left err -> releaseClaimWithError ctxt.pgPoolCT batchUuid err claimList
      Right ok -> do
        markSubmittedBatch ctxt.pgPoolCT ok.providerIDSO ok.batchIDSO claimList
        -- FAST PATH: tell Poll immediately
        ctxt.enqueuePollCT ok.batchIDSO


claimEnteredRequests :: Pool.Pool -> Text -> UUID -> Int -> Int32 -> IO (Vector ClaimedRequest)
claimEnteredRequests pool nodeId batchUuid limitN ttlSec = do
  ei <- Es.execStmt pool $
          Tx.statement (fromIntegral limitN, nodeId, batchUuid, ttlSec) Es.claimRequestsStmt
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right rows -> pure (V.map (uncurry ClaimedRequest) rows)


markSubmittedBatch :: Pool.Pool -> Text -> UUID -> [ClaimedRequest] -> IO ()
markSubmittedBatch pool providerBatchID batchUuid requests = do
  let
    lookupProv rid = case L.find (\cr -> cr.requestIDCR == rid) requests of
      Just cr -> Just cr.requestTextCR
      Nothing -> Nothing
  ei <- Es.execStmt pool $ do
          V.forM_ (V.fromList requests) (\cr -> do
            Tx.statement (cr.requestIDCR, batchUuid, providerBatchID
                  , lookupProv cr.requestIDCR) Es.markSubmittedStmt
            Tx.statement (cr.requestIDCR, "submitted" :: Text
                  , submittedDetails batchUuid (lookupProv cr.requestIDCR))
              Es.insertRequestEventStmt
            )
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()


submittedDetails :: UUID -> Maybe Text -> Value
submittedDetails batchUuid mReqId = Ae.object $ [ 
    "event" Ae..= ("submitted" :: Text)
  , "provider_batch_uuid" Ae..= batchUuid
  ]
  <> case mReqId of
        Nothing   -> []
        Just ridT -> ["provider_request_id" Ae..= ridT]


releaseClaimWithError :: Pool.Pool -> UUID -> SubmitError -> [ClaimedRequest] -> IO ()
releaseClaimWithError pool token submitErr reqs = do
  ei <- Es.execStmt pool $ do
          V.forM_ (V.fromList reqs) (\cr -> do
            Tx.statement (requestIDCR cr, token) Es.releaseClaimStmt
            Tx.statement (requestIDCR cr, "entered" :: Text, failureDetails submitErr.codeSE submitErr.messageSE)
              Es.insertRequestEventStmt
            )
  case ei of
    Left err -> throwIO (DbError (T.pack (show err)))
    Right _  -> pure ()


failureDetails :: Text -> Text -> Value
failureDetails code msg = Ae.object [ 
    "event" Ae..= ("submit_failed" :: Text)
  , "error_code" Ae..= code
  , "error_message" Ae..= msg
  ]
