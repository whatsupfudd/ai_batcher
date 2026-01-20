{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Api.Routing.TopHandlers where

import Servant.Server.Generic (AsServerT, genericServerT)
import Servant.API.Generic (ToServant)

import Api.Types (AIServerApp)
import qualified Api.Routing.ClientHdl as Ch
import Api.Routing.TopDefs


serverApiT :: ToServant TopRoutes (AsServerT AIServerApp)
serverApiT =
  genericServerT $ TopRoutes {
    anonymous = Ch.publicHandlers
    , authenticated = Ch.privateHandlers
  }

