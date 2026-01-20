{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeFamilies #-}

module Api.Routing.TopDefs where

import GHC.Generics (Generic)
import Servant.API ((:>), ReqBody, JSON, Capture, Post, Get)
import Servant.API.Generic ((:-), ToServantApi)
import Servant.Auth.Server (Auth, JWT, Cookie)

import qualified Api.Types as Api
import qualified Api.Routing.ClientRts as Cr


data TopRoutes route = TopRoutes {
    anonymous :: route :- ToServantApi Cr.PublicRoutes
    -- BasicAuth
    , authenticated :: route :- Auth '[JWT, Cookie] Api.ClientInfo :> ToServantApi Cr.PrivateRoutes
  }
  deriving stock (Generic)
