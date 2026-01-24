module Service.Provider where

import qualified Data.ByteString.Lazy as Lbs
import qualified Data.List.NonEmpty as NE
import Data.Text (Text, pack, unpack)
import Data.UUID (UUID)
import Data.UUID as Uu

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
            modelSC = "gpt-5.2"
          , effortSC = "high"
          , systemPromptSC = Nothing
          }
      in
      Oai.submitBatch oaiCfg manager (unpack apiKey) requestPairs
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


pollStatusFromService :: Manager -> Text -> Text -> UUID -> IO (Either String Po.ProviderBatchStatus)
pollStatusFromService manager provider apiKey batchID = do
  case provider of
    "openai" -> do
      eiRez <- Oai.getBatchStatus manager (unpack apiKey) (Uu.toString batchID)
      case eiRez of
        Left errMsg -> pure . Left $ "getBatchStatus err: " <> errMsg
        Right bStatus -> pure . Right $  bStatus
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider


fetchBatchFromService :: Manager -> Text -> Text -> UUID -> IO (Either String Ae.Value)
fetchBatchFromService manager provider apiKey batchID = do
  case provider of
    "openai" -> do
      eiRez <- Oai.getBatchStatus manager (unpack apiKey) (Uu.toString batchID)
      case eiRez of
        Left errMsg -> pure . Left $ "getBatchStatus err: " <> errMsg
        Right bStatus -> pure $ Right $ Ae.object []
    _ -> pure . Left $ "Unsupported provider: " <> unpack provider