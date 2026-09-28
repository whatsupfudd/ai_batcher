module Service.Provider where

import qualified Data.ByteString.Lazy as Lbs
import qualified Data.List.NonEmpty as NE
import Data.Text (Text, pack, unpack)
import Data.UUID (UUID)
import Data.UUID as Uu
import Data.Vector (Vector)
import qualified Data.Vector as V

import System.Environment (getEnv)
import Network.HTTP.Client (Manager)

import qualified Data.Aeson as Ae

import qualified Service.OpenAI as Oai
import qualified Service.OpenAI.Models as OaiM
import Service.OpenAI.Types (ServiceConfig (..)) -- For showing modelSC.
import qualified Service.Types as St
import qualified Engine.Poll as Po


getCredsForProvider :: Text -> IO (Either String Text)
getCredsForProvider provider = do
  case provider of
    "openai" -> Right . pack <$> getEnv "OPENAI_API_KEY"
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


-- Was receiving a NonEmpty (UUID, Text) + un cacheKey.
submitBatchToService :: Manager -> Text -> Text -> Vector St.BatchRequest -> UUID -> Maybe Text -> IO (Either String (Text, UUID))
submitBatchToService manager provider apiKey requests prodID mbModel =
  case provider of
    "openai" ->
      case OaiM.getOpenAIModel mbModel of
        Left errMsg -> pure . Left $ errMsg
        Right oaiCfg -> do
          putStrLn $ "@[submitBatchToService] using model: " <> unpack oaiCfg.modelSC
          if V.null requests then
            pure . Left $ "@[submitBatchToService] no requests to submit"
          else do
            Oai.submitBatch oaiCfg manager (unpack apiKey) requests prodID
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


pollStatusFromService :: Manager -> Text -> Text -> (UUID, Text) -> IO (Either String St.ProviderBatchStatus)
pollStatusFromService manager provider apiKey (batchUid, providerBatchId) = do
  case provider of
    "openai" -> do
      eiRez <- Oai.getBatchStatus manager (unpack apiKey) (batchUid, unpack providerBatchId)
      case eiRez of
        Left errMsg -> pure . Left $ "getBatchStatus err: " <> errMsg
        Right bStatus -> pure . Right $  bStatus
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


fetchBatchFromService :: Manager -> Text -> Text -> (UUID, Text)
    -> IO (Either String (Lbs.ByteString, Vector (Either String St.RequestResult)))
fetchBatchFromService manager provider apiKey (batchUid, providerBatchId) = do
  case provider of
    "openai" -> do
      eiRez <- Oai.fetchBatchResult manager (unpack apiKey) (batchUid, unpack providerBatchId)
      case eiRez of
        Left errMsg -> pure . Left $ "fetchBatchFromService err: " <> errMsg
        Right (rawJson, rez) ->
          let
            listRez = V.map (\(requestID, content, metaData) ->
                case Uu.fromString $ unpack requestID of
                  Just requestUID -> Right $ St.RequestResult requestUID (Just content) (Just metaData)
                  Nothing -> Left $ "Invalid requestID: " <> unpack requestID
              ) rez
          in
          pure $ Right (rawJson, listRez)
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider
