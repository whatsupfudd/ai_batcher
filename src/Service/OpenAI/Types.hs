{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
module Service.OpenAI.Types where

import Data.Text (Text)
import Data.UUID (UUID)

import GHC.Generics (Generic)

import Data.Aeson (Value, FromJSON, ToJSON)


data CacheGeneration =
  NoneCG
  | SimpleCG
  | Inter_55CG
  | Block_56CG
  deriving (Show, Generic)


data CacheRetention =
  K_30minCR
  | K_24hrCR
  | K_MemoryCR
  deriving (Show, Generic)

showRetention :: CacheRetention -> Text
showRetention = \case
  K_30minCR -> "30m"
  K_24hrCR -> "24h"
  K_MemoryCR -> "memory"


data CachePolicy = CachePolicy {
  generationCP :: CacheGeneration
  , retentionCP :: CacheRetention
  }
  deriving (Show, Generic)


data ServiceConfig = ServiceConfig { 
    modelSC :: Text
  , effortSC :: Maybe Text
  , systemPromptSC :: Maybe Text
  , cachePolicySC :: CachePolicy
  {-- To add:
    -- verbosity
    -- max output tokens
    -- store mode.
  --}
  } deriving (Show)
