module DB.Connect where

import Control.Exception (bracket)
import Control.Monad.Cont (ContT (..))
import Control.Monad.IO.Class (liftIO)

import Data.ByteString (ByteString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock (DiffTime)

import GHC.Word (Word16)

import qualified Hasql.Connection as Hc
{-
import qualified Hasql.Connection.Setting as HcS
import qualified Hasql.Connection.Settings.Connection as HcSc
-}
import qualified Hasql.Pool as Hp

data PgDbConfig = PgDbConfig {
  port :: Word16
  , host :: ByteString
  , user :: ByteString
  , passwd :: ByteString
  , dbase :: ByteString
  , poolSize :: Int
  , acqTimeout :: DiffTime
  , poolTimeOut :: DiffTime
  , poolIdleTime :: DiffTime
}
  deriving (Show)


defaultPgDbConf = PgDbConfig {
  port = 5432
  , host = "test"
  , user = "test"
  , passwd = "test"
  , dbase = "test"
  , poolSize = 5
  , acqTimeout = 5
  , poolTimeOut = 60
  , poolIdleTime = 300
  }


startPg :: PgDbConfig -> ContT r IO Hp.Pool
startPg dbC =
  let
    dbSettings = Hc.settings dbC.host dbC.port dbC.user dbC.passwd dbC.dbase
    {-
    pString :: ByteString
    pString = "host=" <> dbC.host <> " port=" <> (T.encodeUtf8 . T.pack) (show dbC.port)
          <> " user=" <> dbC.user <> " password=" <> dbC.passwd <> " dbname=" <> dbC.dbase
    baseSettings = Hp.settings pString
    -}
  in do
  liftIO . putStrLn $ "@[startPg] user: " <> show dbC.user <> " db: " <> show dbC.dbase <> "."
  -- 0.10.1:
  ContT $ bracket (Hp.acquire dbC.poolSize dbC.acqTimeout dbC.poolTimeOut dbC.poolIdleTime dbSettings) Hp.release
  -- 1.3: ContT $ bracket (Hp.acquire settings) Hp.release

