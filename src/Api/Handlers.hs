{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DerivingStrategies #-}

module Api.Handlers where

import Control.Exception (bracket)
import Control.Monad (void)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.Cont (runContT, ContT (..))
import Control.Monad.IO.Class (liftIO)

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Int as DI
import qualified Data.Text as DT
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.List.NonEmpty (NonEmpty (..))

import Data.Aeson (FromJSON (..), ToJSON (..))

import qualified Network.Wai.Handler.Warp as Wrp


import GHC.Generics
import Servant as Srv

import Data.Streaming.Network.Internal (HostPreference (..))

import Servant.Server.Generic (AsServerT, genericServerT)
-- import Hasql.Pool (Pool)
import System.Posix.Signals as Sgnl

import Options.Runtime as Ropt
import Api.Session (loginUser)
import Api.Types


-- TODO: move this into HttpSup section:
setupWai :: Int -> DT.Text -> IO () -> Wrp.Settings
setupWai port host shutdownCallback =
  Wrp.setPort port . Wrp.setHost (Host (DT.unpack host)) . Wrp.setGracefulShutdownTimeout (Just 5) . Wrp.setInstallShutdownHandler shutdownHandler
    -- . setBeforeMainLoop showBanner
    $ Wrp.defaultSettings
  where
  shutdownHandler closeSocket = do
    void $ installHandler Sgnl.sigTERM (Catch $ shutdownCallback >> closeSocket) Nothing
    void $ installHandler Sgnl.sigINT (Catch $ shutdownCallback >> closeSocket) Nothing
