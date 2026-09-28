module Service.OpenAI.Models where

import Data.Text (Text, unpack)

import qualified Service.Types as St
import Service.OpenAI.Types

basicCachePolicy = CachePolicy {
  generationCP = SimpleCG
  , retentionCP = K_30minCR
  }

m56CachePolicy = CachePolicy {
  generationCP = Block_56CG
  , retentionCP = K_24hrCR
  }

nanoOaiCfg = ServiceConfig {
    modelSC = "gpt-5.4-nano"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = basicCachePolicy
  }
miniOaiCfg = ServiceConfig {
    modelSC = "gpt-5.4-mini"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = basicCachePolicy
  }
advOaiCfg_5_4 = ServiceConfig {
    modelSC = "gpt-5.4"
  , effortSC = Just "high"
  , systemPromptSC = Nothing
  , cachePolicySC = basicCachePolicy
  }
advOaiCfg_5_5 = ServiceConfig {
    modelSC = "gpt-5.5"
  , effortSC = Just "high"
  , systemPromptSC = Nothing
  , cachePolicySC = CachePolicy {
      generationCP = Inter_55CG
      , retentionCP = K_30minCR
    }
  }
lunaOaiCfg_5_6 = ServiceConfig {
    modelSC = "gpt-5.6-luna"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }
terraOaiCfg_5_6 = ServiceConfig {
    modelSC = "gpt-5.6-terra"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }
solOaiCfg_5_6 = ServiceConfig {
    modelSC = "gpt-5.6-sol"
  , effortSC = Just "high"
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }
lunaOaiCfg_6 = ServiceConfig {
    modelSC = "gpt-6-luna"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }
solOaiCfg_6 = ServiceConfig {
    modelSC = "gpt-6-sol"
  , effortSC = Nothing
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }
astraOaiCfg_6 = ServiceConfig {
    modelSC = "gpt-6-astra"
  , effortSC = Just "high"
  , systemPromptSC = Nothing
  , cachePolicySC = m56CachePolicy
  }


getOpenAIModel :: Maybe Text -> Either String ServiceConfig
getOpenAIModel mbModel =
  case mbModel of
  Nothing -> Right nanoOaiCfg
  Just model -> case model of
    "gpt5.4-nano" -> Right nanoOaiCfg
    "gpt5.4-mini" -> Right miniOaiCfg
    "gpt5.4-high" -> Right advOaiCfg_5_4
    "gpt5.5-high" -> Right advOaiCfg_5_5
    "gpt5.6" -> Right solOaiCfg_5_6
    "gpt5.6-luna" -> Right lunaOaiCfg_5_6
    "gpt5.6-luna-high" -> Right $ lunaOaiCfg_5_6 { effortSC = Just "high" }
    "gpt5.6-luna-low" -> Right $ lunaOaiCfg_5_6 { effortSC = Just "low" }
    "gpt5.6-luna-medium" -> Right $ lunaOaiCfg_5_6 { effortSC = Just "medium" }
    "gpt5.6-terra" -> Right terraOaiCfg_5_6
    "gpt5.6-terra-high" -> Right $ terraOaiCfg_5_6 { effortSC = Just "high" }
    "gpt5.6-terra-low" -> Right $ terraOaiCfg_5_6 { effortSC = Just "low" }
    "gpt5.6-terra-medium" -> Right $ terraOaiCfg_5_6 { effortSC = Just "medium" }
    "gpt5.6-sol" -> Right solOaiCfg_5_6
    "gpt5.6-sol-high" -> Right $ solOaiCfg_5_6 { effortSC = Just "high" }
    "gpt5.6-sol-low" -> Right $ solOaiCfg_5_6 { effortSC = Just "low" }
    "gpt5.6-sol-medium" -> Right $ solOaiCfg_5_6 { effortSC = Just "medium" }
    "gpt6-luna" -> Right lunaOaiCfg_6
    "gpt6-sol" -> Right solOaiCfg_6
    "gpt6-astra" -> Right astraOaiCfg_6
    _ -> Left $ "Unsupported model: " <> unpack model
