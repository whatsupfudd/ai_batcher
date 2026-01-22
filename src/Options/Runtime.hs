module Options.Runtime (defaultRun, RunOptions (..), PgDbConfig (..), defaultPgDbConf) where

import Data.Text (Text)

import HttpSup.CorsPolicy (CORSConfig, defaultCorsPolicy)
import DB.Connect (PgDbConfig (..), defaultPgDbConf)
import Assets.Types (S3Config (..), defaultS3Conf)


data RunOptions = RunOptions {
    debug :: Int
    , corsPolicy :: Maybe CORSConfig
    , jwkConfFile :: Maybe FilePath
    , serverPort :: Int
    , serverHost :: Text
    , pgDbConf :: PgDbConfig
    , s3store :: Maybe S3Config
    , provider :: Text
  }
  deriving (Show)

defaultRun :: FilePath -> Text -> Int -> RunOptions
defaultRun homeDir server port = RunOptions {
    debug = 0
    , corsPolicy = Just defaultCorsPolicy
    , jwkConfFile = Just (homeDir <> "/jwkConf.json")
    , serverHost = server
    , serverPort = port
    , pgDbConf = defaultPgDbConf
    , s3store = Nothing
    , provider = "openai"
  }
