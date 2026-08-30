module DB.Connect where

import Control.Exception (bracket)
import Control.Monad.Cont (ContT (..))
import Control.Monad.IO.Class (liftIO)

import Data.ByteString (ByteString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as Te
import Data.Time.Clock (DiffTime)

import GHC.Word (Word16)

import Hasql.Pool (Pool, acquire, release)
import qualified Hasql.Pool.Config as Pc
import qualified Hasql.Connection.Setting.Connection.Param as Cp
import qualified Hasql.Connection.Setting.Connection as Csc
import qualified Hasql.Connection.Setting as Cs


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


startPg :: PgDbConfig -> ContT r IO Pool
startPg dbC =
  let
    connParams = [Cp.host $ Te.decodeUtf8 dbC.host, Cp.port dbC.port, Cp.user $ Te.decodeUtf8 dbC.user, Cp.password $ Te.decodeUtf8 dbC.passwd, Cp.dbname $ Te.decodeUtf8 dbC.dbase]
    csSetting = Cs.connection $ Csc.params connParams
    pcSetting = Pc.staticConnectionSettings [ csSetting ]
    poolSettings = [Pc.size dbC.poolSize, Pc.acquisitionTimeout dbC.acqTimeout, Pc.agingTimeout dbC.poolTimeOut, Pc.acquisitionTimeout dbC.poolIdleTime]
    dbConfig = Pc.settings (pcSetting : poolSettings)
  in do
  liftIO . putStrLn $ "@[startPg] user: " <> show dbC.user <> " db: " <> show dbC.dbase <> "."
  ContT $ bracket (acquire dbConfig) release