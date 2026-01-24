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

import qualified Service.Types as St
import qualified Service.OpenAI as Oai
import qualified Engine.Poll as Po

getCredsForProvider :: Text -> IO (Either String Text)
getCredsForProvider provider = do
  case provider of
    "openai" -> Right . pack <$> getEnv "OPENAI_API_KEY"
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


submitBatchToService :: Manager -> Text -> Text -> NE.NonEmpty (UUID, Text) -> IO (Either String (Text, UUID))
submitBatchToService manager provider apiKey requestPairs = do
  case provider of
    "openai" ->
      let oaiCfg = St.ServiceConfig {
            modelSC = "gpt-4.1-nano"   -- "gpt-5.2"
          , effortSC = Nothing -- Just "minimal"      -- "high"
          , systemPromptSC = Nothing
          }
      in
      Oai.submitBatch oaiCfg manager (unpack apiKey) requestPairs
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


fetchBatchFromService :: Manager -> Text -> Text -> (UUID, Text) -> IO (Either String (Lbs.ByteString, Vector (Either String St.RequestResult)))
fetchBatchFromService manager provider apiKey (batchUid, providerBatchId) = do
  case provider of
    "openai" -> do
      eiRez <- Oai.fetchBatchResult manager (unpack apiKey) (batchUid, unpack providerBatchId)
      case eiRez of
        Left errMsg -> pure . Left $ "fetchBatchFromService err: " <> errMsg
        Right (rawJson, rez) ->
          let
            listRez = V.map (\(requestID, content) ->
                case Uu.fromString $ unpack requestID of
                  Just requestUID -> Right $ St.RequestResult requestUID (Just content) Nothing
                  Nothing -> Left $ "Invalid requestID: " <> unpack requestID
              ) rez
          in 
          pure $ Right (rawJson, listRez)
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider