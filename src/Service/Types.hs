{-# LANGUAGE DeriveGeneric #-}
module Service.Types where

import Data.Text (Text)
import Data.UUID (UUID)

import GHC.Generics (Generic)

import Data.Aeson (Value, FromJSON, ToJSON)


data ServiceConfig = ServiceConfig { 
    modelSC :: Text
  , effortSC :: Maybe Text
  , systemPromptSC :: Maybe Text
  } deriving (Show)


data ProviderBatchStatus = 
    BatchRunning
  | BatchFinalizing
  | BatchInProgress
  | BatchValidating
  | BatchCompleted
  | BatchCancelled Text
  | BatchFailed Text
  deriving (Show, Eq, Generic)


data RequestResult = RequestResult {
  requestId :: UUID
  , content :: Maybe Text
  , metaData :: Maybe Value
} deriving (Show, Eq, Generic)