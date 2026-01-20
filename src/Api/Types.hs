{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}

module Api.Types where

import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.Except (ExceptT, withExceptT, MonadError)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader, ReaderT)

import qualified Data.ByteString as Bs
import qualified Data.ByteString.Lazy as Lbs
import Data.Int (Int32)
import qualified Data.Map as Mp
import Data.Text (Text)
import Data.Time (UTCTime, getCurrentTime)
import Data.Text.Encoding (encodeUtf8)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)

import GHC.Generics

import Data.Aeson (FromJSON, ToJSON)

import Network.HTTP.Media ((//), (/:))

import Servant.Auth.Server (Auth, AuthResult
          , JWT, JWTSettings, FromJWT, ToJWT
          -- , BasicAuth, BasicAuthData, BasicAuthCfg, FromBasicAuthData (..)
        )
import Servant.Server
import Servant.Server.Generic (AsServerT, genericServerT)
import Servant.API.Generic ((:-), ToServantApi, ToServant)
import Servant.API ((:>), ReqBody, JSON, Capture, Post, Get)
import Servant.API.ContentTypes (Accept (..), MimeRender (..), PlainText)

import Hasql.Pool (Pool)

import qualified Options.Runtime as Rt
import qualified Assets.Types as S3


-- Client Data going in / out.
data ClientInfo = ClientInfo {
    uid :: Int32
    , eid :: UUID
    , expiry :: UTCTime
  }
  deriving stock (Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

instance ToJWT ClientInfo
instance FromJWT ClientInfo
{-
instance FromBasicAuthData ClientInfo where
  fromBasicAuthData authData authChecker = authChecker authData
-}

fakeClientInfo :: IO ClientInfo
fakeClientInfo = do
  now <- getCurrentTime
  uuid <- nextRandom
  pure $ ClientInfo {
      uid = 1
      , eid = uuid
      , expiry = now
    }


data SessionItems = SessionItems {
    clientInfo :: ClientInfo
    , connID :: Int32
    , jwt :: Text
  }
  deriving stock (Show, Generic)
  deriving anyclass (ToJSON)


-- type instance BasicAuthCfg = BasicAuthData -> IO (AuthResult ClientInfo)


data HTML = HTML
newtype Html = Html { html :: Bs.ByteString }


instance Accept HTML where
  contentType _ = "text" // "html" /: ("charset", "utf-8")

instance MimeRender HTML Html where
  mimeRender _ = Lbs.fromStrict . html

instance MimeRender PlainText Html where
  mimeRender _ = Lbs.fromStrict . html

-- Context to all API handlers (ReaderT)

data AppEnv = AppEnv {
    config_Ctxt :: Rt.RunOptions
    , jwt_Ctxt :: JWTSettings
    , pgPool_Ctxt :: Pool
    , s3Storage_Ctxt :: Maybe S3.S3Conn
  }


-- Error management:
data ApiError =
  DoesNotExistAE Text
  | InternalErrorAE Text
  | UnauthorizedAE
  | UnknownSituationAE Text
  deriving stock (Generic, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)


asServantError :: ApiError -> ServerError
asServantError error =
  case error of
    DoesNotExistAE  msg-> err404 { errBody = Lbs.fromStrict . encodeUtf8 $ msg }
    InternalErrorAE msg -> err500 { errBody = Lbs.fromStrict . encodeUtf8 $ msg }
    UnauthorizedAE -> err401 { errBody = "Unauthorized access." }
    UnknownSituationAE msg -> err500 { errBody = Lbs.fromStrict . encodeUtf8 $ msg }


-- Main Application / handler for the API:
newtype AIServerApp api = AIServerApp {
     -- apiHandler :: RWST AppEnv (Set Ws.ErrMessage) Ws.WappState (ExceptT ApiError IO) api
     apiHandler :: ReaderT AppEnv (ExceptT ApiError IO) api
  }
  deriving newtype ( 
      Functor,
      Applicative,
      Monad,
      MonadMask,
      MonadCatch,
      MonadThrow,
      MonadReader AppEnv,
      MonadIO,
      MonadError ApiError
      -- , MonadState Ws.WappState
    )
