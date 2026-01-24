module Commands.Producer (produceCmd) where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- 

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

import qualified Data.Aeson as Ae

import Network.HTTP.Client.TLS ( tlsManagerSettings )
import Network.HTTP.Client as Hc

import qualified Hasql.Pool as Pool

import qualified DB.Connect as Db
import qualified Assets.S3Ops as S3
import qualified Assets.Types as At
import qualified Options.Cli as Cl
import qualified Options.Runtime as Opt
import qualified Assets.Template as Tp
import qualified Engine.Runner as R
import qualified Engine.Submit as Su
import qualified Engine.Poll as Po
import qualified Engine.Fetch as Fe
import qualified Service.Provider as Sp
import Control.Concurrent (threadDelay)


produceCmd :: Cl.ProducerOpts -> Opt.RunOptions -> IO ()
produceCmd prodOpts rtOpts = do
  putStrLn $ "Ingesting product: " <> T.unpack prodOpts.productIG
  putStrLn $ "Version: " <> show prodOpts.versionIG
  case S3.makeS3Conn <$> rtOpts.s3store of
    Nothing -> putStrLn "No S3 connection found"
    Just s3Conn -> do
      runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool ->
        let
          ctxt = Tp.Context {
              pgPoolCT = pgPool
            , s3RepoCT = storeS3 s3Conn
            }
          targetProvider = fromMaybe rtOpts.provider prodOpts.providerIG
        in do
        eiApiKey <- Sp.getCredsForProvider targetProvider
        case eiApiKey of
          Left errMsg -> putStrLn $ "@[produceCmd] getCredsForProvider err: " <> errMsg
          Right apiKey -> do
            rezA <- Tp.ingestTemplate ctxt prodOpts
            case rezA of
              Left err -> putStrLn $ "@[produceCmd] ingestTemplate err: " <> show err
              Right productionID -> do
                putStrLn $ "@[produceCmd] productionID: " <> show productionID
                runEngines pgPool s3Conn apiKey targetProvider productionID
      pure ()


storeS3 :: At.S3Conn -> Lbs.ByteString -> IO UUID
storeS3 s3Conn bytes = do
  newUUID <- nextRandom
  S3.putFromText s3Conn (T.pack . toString $ newUUID) (Lbs.toStrict bytes) Nothing
  pure newUUID


runEngines :: Pool.Pool -> At.S3Conn -> T.Text -> T.Text -> UUID -> IO ()
runEngines pgPool s3Conn apiKey targetProvider productionID = do
  manager <- Hc.newManager tlsManagerSettings
  let
    fetchCtxt = Fe.Context {
          pgPoolCT = pgPool
        , nodeIdCT = "producer"
        , s3RepoCT = storeS3 s3Conn
        , fetchBatchCT = fetchBatchFromService manager
        , enqueueGenDocCT = Nothing
        }
    fetchCfg = Fe.FetchConfig {
          pollOutboxIntervalMicrosFC = 1000000
        , maxBatchesPerTickFC = 10
        , queueDepthFC = 100
        , workerCountFC = 10
        , claimTtlSecondsFC = 300
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
          pollIntervalMicrosPC = 1000000
        , maxBatchesPerTickPC = 10
        , queueDepthPC = 100
        , workerCountPC = 10
        , claimTtlSecondsPC = 300
        }
  poll <- Po.startPollEngine pollCtxt pollCfg
  putStrLn "@[runEngines] started poll."
  let
    submitCtxt = Su.Context {
          pgPoolCT = pgPool
        , nodeIdCT = "producer"
        , sendRequestCT = submitBatchToService manager targetProvider
        , enqueuePollCT = poll.enqueuePH
        }
    submitCfg = Su.SubmitConfig {
          pollIntervalMicrosSC = 1000000
        , batchSizeSC = 10
        , queueDepthSC = 100
        , workerCountSC = 10
        , claimTtlSecondsSC = 300
        }
  submit <- Su.startSubmitEngine submitCtxt submitCfg
  putStrLn "@[runEngines] started submit."
  -- TODO: how long to run?
  threadDelay $ 1000000 * 10
  where  
  submitBatchToService :: Manager -> Text -> NonEmpty (UUID, Text) -> IO (Either Su.SubmitError Su.SubmitOk)
  submitBatchToService manager targetProvider requestPairs = do
    eiRez <- Sp.submitBatchToService manager targetProvider apiKey requestPairs
    case eiRez of
      Left errMsg -> pure . Left $ Su.SubmitError ("P:" <> targetProvider) (T.pack errMsg)
      Right (providerID, batchID) ->
        pure . Right $ Su.SubmitOk providerID batchID


  pollStatusFromService :: Manager -> UUID -> IO (Either Po.PollError Po.ProviderBatchStatus)
  pollStatusFromService manager batchID = do
    rez <- Sp.pollStatusFromService manager targetProvider apiKey batchID
    case rez of
      Left errMsg -> pure . Left $ Po.PollError ("P:" <> targetProvider) (T.pack errMsg)
      Right value -> pure $ Right value


  fetchBatchFromService :: Manager -> UUID -> IO (Either Fe.FetchError Ae.Value)
  fetchBatchFromService manager batchID = do
    eiRez <- Sp.fetchBatchFromService manager targetProvider apiKey batchID
    case eiRez of
      Left errMsg -> pure . Left $ Fe.FetchError ("P:" <> targetProvider) (T.pack errMsg)
      Right value -> pure . Right $ value


nePartitionEithers :: NonEmpty (Either a b) -> ([a], [b])
nePartitionEithers =
  foldr (\eiItem (leftAccum, rightAccum) -> case eiItem of
    Left aVal -> (aVal : leftAccum, rightAccum)
    Right bVal -> (leftAccum, bVal : rightAccum)
  ) ([], [])