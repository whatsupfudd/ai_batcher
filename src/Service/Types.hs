{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
module Service.Types where

import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)
import Data.UUID (UUID)

import GHC.Generics (Generic)

import Data.Aeson (Value)
import qualified Data.Aeson as Ae

import qualified Service.OpenAI.Cache as Ch
import Utils as Ut


data ProviderBatchStatus = 
    BatchRunning
  | BatchFinalizing
  | BatchInProgress
  | BatchValidating
  | BatchCompleted
  | BatchCancelled Text
  | BatchFailed Text
  deriving (Show, Eq, Generic)

data PromptRole =
  SystemPR
  | UserPR
  | DeveloperPR
  deriving (Show, Eq, Generic)

showRolePR :: PromptRole -> Text
showRolePR = \case
  SystemPR -> "system"
  UserPR -> "user"
  DeveloperPR -> "developer"


-- RequestContext: the "memory" supporting the content.
data RequestContext = RequestContext {
  memoryPC :: Text
  , rolePC :: PromptRole
  }
  deriving (Show)

-- RequestContent: the actual generative comment sent to the model.
data RequestMessage = RequestMessage {
  idRP :: UUID
  , contentPR :: Text
  }
  deriving (Show)


data BatchRequest = BatchRequest {
  idBR :: UUID
  , contextBR :: [RequestContext]
  , messagesBR :: NonEmpty RequestMessage
  }
  deriving (Show)



data RequestResult = RequestResult {
  requestId :: UUID
  , content :: Maybe Text
  , metaData :: Maybe MetaInfo
} deriving (Show, Eq, Generic)


data MetaInfo = MetaInfo {
    usageMt :: Maybe Value
  , modelMt :: Maybe Text
  , respIdMt :: Maybe Text
  }
  deriving (Show, Eq, Generic)

instance Ae.ToJSON MetaInfo where
  toJSON aMetaInfo =
    Ae.object [
      "usage" Ae..= aMetaInfo.usageMt
      , "model" Ae..= aMetaInfo.modelMt
      , "respId" Ae..= aMetaInfo.respIdMt
    ]


calcCache :: [RequestContext] -> Text
calcCache memories =
  let
    allText = foldl (\acc mm -> acc <> mm.memoryPC) "" memories
    hash = Ut.toHex64Text $ Ut.fnv1a64Text allText
  in
    "doc-fnv1a64-" <> hash
  