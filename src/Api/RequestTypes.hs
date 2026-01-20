{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds      #-}

module Api.RequestTypes where

import Data.Text (Text)
import Data.UUID (UUID)
import Data.Time.Clock (UTCTime)

import GHC.Generics (Generic)

import Data.Aeson (FromJSON, ToJSON, Value)

import Servant.API (NoContent)


data LoginForm = LoginForm {
    username :: Text
    , secret :: Text
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)


newtype LoginRequest = SimpleLR LoginForm


data OperationStatus = OperationStatus { 
    requestId :: UUID
  , status :: Text
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

-- | 4. Service invocation
data InvokeRequest = InvokeRequest { 
    function :: UUID
  , context :: Maybe UUID
  , parameters :: Value
  , content :: Value
  , files :: [UUID]
  , references :: [UUID]
  } deriving (Show, Eq, Generic, FromJSON)

data ResourceRequest = ResourceRequest { 
    function :: UUID
  , parameters :: Value
  } deriving (Show, Eq, Generic, FromJSON)

-- | 5. Storage
data StoragePut = StoragePut { 
    name :: Text
  , mimeType :: Text
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

data StorageMeta = StorageMeta { 
    itemId :: UUID
  , location :: Text
  , created :: UTCTime
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)
