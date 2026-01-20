{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}

module Api.Routing.ClientRts where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.UUID (UUID)

import GHC.Generics (Generic)

import Servant.API.Generic
import Servant.API (JSON, PlainText, ReqBody, Get, Post, Delete, OctetStream
          , (:>), Capture, QueryParam,QueryParam', Optional, Raw, Header, Headers)
import Servant.Auth.Server (Auth, JWT, BasicAuth)

import Api.Types (ClientInfo, Html)
import qualified Api.RequestTypes as Rq
import qualified Api.ResponseTypes as Rr


data PublicRoutes route = PublicRoutes {
    loginPR :: route :- "login" :> ReqBody '[JSON] Rq.LoginForm :> Post '[JSON] Rr.LoginResult
  }
  deriving (Generic)


data PrivateRoutes route = PrivateRoutes {
  client :: route :- "client" :> ToServantApi ClientRoutes
  , invoke :: route :- "invoke" :> ToServantApi InvokeRoutes
  , asset :: route :- "asset" :> ToServantApi AssetRoutes
  , retrieve :: route :- "retrieve" :> ToServantApi RetrieveRoutes
  , storage :: route :- "storage" :> ToServantApi StorageRoutes
  }
  deriving (Generic)


data ClientRoutes route = ClientRoutes {
      getClient :: route :- Capture "clientId" UUID :> Get '[JSON] ClientInfo
  }
  deriving (Generic)

data InvokeRoutes route = InvokeRoutes {
  invokeService :: route :- ReqBody '[JSON] Rq.InvokeRequest :> Post '[JSON] Rr.InvokeResponse
  }
  deriving (Generic)

newtype AssetRoutes route = AssetRoutes {
  getAsset :: route :-  Capture "assetId" UUID :> QueryParam' '[Optional] "p" Text :> Get '[OctetStream] (Headers '[Header "Content-Type" Text] ByteString)
  }
  deriving (Generic)

newtype RetrieveRoutes route = RetrieveRoutes {
  retrieve :: route :- Capture "requestId" UUID :> Get '[JSON] Rr.InvokeResponse
  }
  deriving (Generic)

data StorageRoutes route = StorageRoutes {
    storagePut :: route :- ReqBody '[JSON] Rq.StoragePut :> Post '[JSON] Rr.ReplyClientRequest
  , storageGet :: route :- Capture "itemId" UUID :> Get '[OctetStream] ByteString
  , storageDel :: route :- Capture "itemId" UUID :> Delete '[JSON] Rr.ReplyClientRequest
  }
  deriving (Generic)
