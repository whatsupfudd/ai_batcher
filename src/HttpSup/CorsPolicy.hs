{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module HttpSup.CorsPolicy where
import Data.Aeson (FromJSON, ToJSON)
import Data.CaseInsensitive as CI
-- import Data.List (elem, lookup)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as DT
import Data.Text.Encoding (encodeUtf8)
import GHC.Generics (Generic)
import Network.HTTP.Types.Header (hOrigin)
import Network.Wai
import Network.Wai.Middleware.Cors
-- import Prelude


defaultCorsPolicy :: CORSConfig
data CORSConfig = CORSConfig {
   allowedOrigins :: [ Text ]
  , publicPrefixes :: [ Text ]
  , maxAge :: Maybe Int
  }
  deriving stock (Generic, Show)
  deriving anyclass (ToJSON, FromJSON)


defaultCorsPolicy = CORSConfig {
   allowedOrigins = [ "http://localhost", "http://localhost:8885" ]
  , publicPrefixes = [ "login", "site", "/", "index" ]
  , maxAge = Nothing
  }


setCorsPolicy :: CORSConfig -> Middleware
setCorsPolicy CORSConfig{..} = cors $ \request ->
  if isPublicApi request
  then Just $ simpleCorsResourcePolicy {
    corsMethods = simpleMethods <> [ "OPTIONS" ]
    , corsRequestHeaders = simpleHeaders <> ["Authorization", "Content-Type"]
    }
  else Just $ CorsResourcePolicy {
    corsOrigins = matchHostOrigin request
    , corsMethods = simpleMethods <> [ "DELETE", "OPTIONS" ]
    , corsRequestHeaders = ["Authorization", "Content-Type"]
    , corsExposedHeaders = Nothing
    , corsMaxAge = maxAge
    , corsVaryOrigin = True
    , corsRequireOrigin = True
    , corsIgnoreFailures = False
    }
    where
      matchHostOrigin req = Just . fromMaybe ([], False) $ do
        -- Maybe monad: CORS will be denied when there's any Nothing.
        _vhost <- requestHeaderHost req
        origin <- lookup hOrigin (requestHeaders req)

        if CI.mk origin `elem` ciAllowedOrigins
        then return ([origin], {-allow credentials?-} True)
        else Nothing

      isPublicApi req = case pathInfo req of
        []    -> False
        (x:_) -> any (`DT.isPrefixOf` x) publicPrefixes

      ciAllowedOrigins = mk . encodeUtf8 <$> allowedOrigins
