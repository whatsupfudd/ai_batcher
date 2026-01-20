{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}

module Api.Serve where

import Control.Monad.Except (ExceptT, MonadError, withExceptT)
import Control.Monad.Reader (runReaderT)
import Control.Monad.IO.Class (liftIO)

import Data.List.NonEmpty (NonEmpty (..))
import Data.Text (unpack)
import qualified Data.Map as Mp
import GHC.Generics

import Servant (Proxy (..), Context (..), hoistServerWithContext, serveWithContext, Handler (..))
import Servant.Server.Generic (AsServerT, genericServerT)
import Servant.Auth.Server (Auth, AuthResult (..), IsSecure (NotSecure)
            -- , BasicAuth, BasicAuthCfg, FromBasicAuthData
            , CookieSettings (..), defaultCookieSettings
            , JWT, JWTSettings, FromJWT (..), ToJWT (..), defaultJWTSettings )
import qualified Servant.Auth.Server as Sauth
import Servant.API ((:>), JSON)
import Servant.API.Generic ((:-), ToServantApi, ToServant)
import Servant.Multipart (defaultMultipartOptions, MultipartOptions (..), Tmp, Mem)

import Network.Wai (Application)
import Network.Wai.Parse (setMaxRequestKeyLength, defaultParseRequestBodyOptions, setMaxRequestFileSize, setMaxRequestFilesSize)
import Network.Wai.Middleware.RequestLogger (logStdout)

import Hasql.Pool (Pool)

import HttpSup.JWT (readJWK, tmpJWK)
import HttpSup.CorsPolicy (setCorsPolicy)
-- Old stuff:
import Api.Session (validateUser)
-- New routing system:
import Api.Routing.TopDefs (TopRoutes)
import Api.Routing.TopHandlers (serverApiT)
import qualified Assets.Types as S3
import qualified Assets.S3Ops as S3
import Assets.Types (S3Config (..))
import qualified Options.Runtime as Rt
import Api.Types


-- Root handling definition:
type MainApi = ToServantApi TopRoutes

endpointsProxy :: Proxy MainApi
endpointsProxy = Proxy


launchServant ::  Rt.RunOptions -> Pool -> IO Application
launchServant rtOpts pgPool = do
  putStrLn $ "@[launchServant] starting, confFile: " <> show rtOpts.jwkConfFile <> "."
  -- TODO: figure out how to turn off JWT authentication.
  jwtKey <- maybe tmpJWK readJWK rtOpts.jwkConfFile
  putStrLn "@[serveApi] got jwt keys."

  let
    linkUp :: NonEmpty (a -> a) -> a -> a
    linkUp = foldr1 (.)

    cookieSettings = defaultCookieSettings -- { cookieIsSecure = NotSecure }
    jwtSettings = Sauth.defaultJWTSettings jwtKey
    sessionAuth = validateUser pgPool jwtSettings
    -- sessionAuth = validateUser
    -- For file upload support, will be used later:
    multipartOpts :: MultipartOptions Tmp
    multipartOpts = (defaultMultipartOptions (Proxy :: Proxy Tmp)) {
        generalOptions = setMaxRequestFilesSize 50000000 
            $ setMaxRequestFileSize 50000000
            $ setMaxRequestKeyLength 1024 defaultParseRequestBodyOptions
      }
    -- multipartOpts :.
    -- sessionAuth :.
    apiContext = cookieSettings :. jwtSettings :. sessionAuth :. EmptyContext
    -- BasicAuthCfg
    --  , MultipartOptions Tmp
    apiContextP = Proxy :: Proxy '[CookieSettings, JWTSettings]

    middlewares = case rtOpts.corsPolicy of
      Nothing -> linkUp $ id :| [ logStdout ]
      Just aPolicy -> linkUp $ id :| [ logStdout, setCorsPolicy aPolicy ]
    s3Storage = S3.makeS3Conn <$> rtOpts.s3store
    appEnv = AppEnv { config_Ctxt = rtOpts
          , jwt_Ctxt = jwtSettings
          , pgPool_Ctxt = pgPool
          , s3Storage_Ctxt = s3Storage
        }
    -- TODO: add state to the handler if running with RWST.
    server = hoistServerWithContext endpointsProxy apiContextP (apiAsHandler appEnv) serverApiT

  case s3Storage of
    Nothing -> putStrLn "@[serveApi] no s3 storage configured."
    Just aConn -> do
      putStrLn $ "@[serveApi] using s3, host: " <> maybe "<error!>" (unpack . S3.host) rtOpts.s3store <> ", bucket: " <> show aConn.bucketCn

  putStrLn $ "@[serveApi] listening on port " <> show rtOpts.serverPort <> "."
  pure $ middlewares $ serveWithContext endpointsProxy apiContext server


-- Manages the application environment and projects to the Handler after a request processing. It is invoked for each request.
-- apiAsHandler :: AppEnv -> WappState -> EasyVerseApp api -> Handler api
-- apiAsHandler env state app =
apiAsHandler :: AppEnv -> AIServerApp api -> Handler api
apiAsHandler env = do
  Handler . withExceptT asServantError . flip runReaderT env . apiHandler
  {- With state:
  Handler $ withExceptT asServantError
    ((\(a, _, _) -> a) <$> runRWST (apiHandler app) env state)
  --}