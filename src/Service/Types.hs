{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
module Service.Types where

import Data.Text (Text)

import GHC.Generics (Generic)

import Data.Aeson (Value, FromJSON, ToJSON)


data ServiceConfig = ServiceConfig { 
    modelSC :: Text
  , effortSC :: Text
  , systemPromptSC :: Maybe Text
  } deriving (Show)


data ProviderBatchStatus = 
    BatchRunning
  | BatchCompleted
  | BatchCancelled Text
  | BatchFailed Text
  deriving (Show, Eq, Generic)
