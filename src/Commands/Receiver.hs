module Commands.Receiver (receiveCmd) where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- 
import Control.Concurrent (threadDelay)

import qualified Data.ByteString.Lazy as Lbs
import Data.Either (lefts, rights)
import Data.Maybe (fromMaybe)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.UUID (UUID, toString)
import Data.UUID.V4 (nextRandom)
import qualified Data.UUID as Uu
import Data.Vector (Vector)

import qualified Data.Aeson as Ae

import Network.HTTP.Client.TLS ( tlsManagerSettings )
import Network.HTTP.Client as Hc

import qualified Hasql.Pool as Pool

import qualified DB.Connect as Db
import qualified Assets.S3Ops as S3
import qualified Assets.Storage as St
import qualified Assets.Types as At
import qualified Options.Cli as Cl
import qualified Options.Runtime as Opt
import qualified Assets.Template as Tp
import qualified Engine.Runner as R
import qualified Engine.Submit as Su
import qualified Engine.Poll as Po
import qualified Engine.Fetch as Fe
import qualified Service.Provider as Sp
import qualified Service.Types as St


receiveCmd :: Cl.ReceiverOpts -> Opt.RunOptions -> IO ()
receiveCmd recvOpts rtOpts = do
  case S3.makeS3Conn <$> rtOpts.s3store of
    Nothing -> putStrLn "No S3 connection found"
    Just s3Conn -> do
      runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool ->
        let
          ctxt = Tp.Context {
              pgPoolCT = pgPool
            , s3RepoCT = St.storeS3 s3Conn
            , s3FetchCT = St.fetchTemplateFromS3 s3Conn
            }
        in do
        eiApiKey <- Sp.getCredsForProvider recvOpts.providerRC
        case eiApiKey of
          Left errMsg -> putStrLn $ "@[produceCmd] getCredsForProvider err: " <> errMsg
          Right apiKey -> do
            runEngines pgPool s3Conn apiKey recvOpts.providerRC
      pure ()


runEngines :: Pool.Pool -> At.S3Conn -> T.Text -> T.Text -> IO ()
runEngines pgPool s3Conn apiKey targetProvider = do
  manager <- Hc.newManager tlsManagerSettings
  let
    fetchCtxt = Fe.Context {
          pgPoolCT = pgPool
        , nodeIdCT = "producer"
        , s3RepoCT = St.storeS3 s3Conn
        , fetchBatchCT = fetchBatchFromService manager
        , enqueueGenDocCT = Nothing
        }
    fetchCfg = Fe.FetchConfig {
          pollOutboxIntervalMicrosFC = 1000000
        , maxBatchesPerTickFC = 10
        , queueDepthFC = 100
        , workerCountFC = 10
        , claimTtlSecondsFC = 60
        , errorBackoffSecondsFC = 300
        }
  fetcher <- Fe.startFetchEngine fetchCtxt fetchCfg
  putStrLn "@[runEngines] started fetcher."
  let
    pollCtxt = Po.Context {
          pgPoolCT = pgPool
        , nodeIdCT = "producer"
        , pollStatusCT = pollStatusFromService manager
        , enqueueFetchCT = fetcher.enqueueFH
        }
    pollCfg = Po.PollConfig {
          pollIntervalMicrosPC = 1000000 * 10 -- 10 seconds
        , maxBatchesPerTickPC = 10
        , queueDepthPC = 100
        , workerCountPC = 10
        , claimTtlSecondsPC = 60
        }
  poll <- Po.startPollEngine pollCtxt pollCfg
  putStrLn "@[runEngines] started poll."
  -- TODO: how long to run?
  threadDelay $ 1000000 * 60 * 10 -- 10 minutes
  where
  pollStatusFromService :: Manager -> (UUID, Text) -> IO (Either Po.PollError St.ProviderBatchStatus)
  pollStatusFromService manager (batchUid, providerBatchId) = do
    rez <- Sp.pollStatusFromService manager targetProvider apiKey (batchUid, providerBatchId)
    case rez of
      Left errMsg -> pure . Left $ Po.PollError ("P:" <> targetProvider) (T.pack errMsg)
      Right value -> pure $ Right value


  fetchBatchFromService :: Manager -> (UUID, Text) -> IO (Either Fe.FetchError (Lbs.ByteString, Vector (Either String St.RequestResult)))
  fetchBatchFromService manager (batchUid, providerBatchId) = do
    eiRez <- Sp.fetchBatchFromService manager targetProvider apiKey (batchUid, providerBatchId)
    case eiRez of
      Left errMsg -> pure . Left $ Fe.FetchError ("P:" <> targetProvider) (T.pack errMsg)
      Right value -> pure . Right $ value


nePartitionEithers :: NonEmpty (Either a b) -> ([a], [b])
nePartitionEithers =
  foldr (\eiItem (leftAccum, rightAccum) -> case eiItem of
    Left aVal -> (aVal : leftAccum, rightAccum)
    Right bVal -> (leftAccum, bVal : rightAccum)
  ) ([], [])
