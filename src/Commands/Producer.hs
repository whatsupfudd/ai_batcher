module Commands.Producer (produceCmd) where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- 

import qualified Data.ByteString as Bs
import qualified Data.Text as T
import Data.UUID (UUID, toString)
import Data.UUID.V4 (nextRandom)

import qualified DB.Connect as Db
import qualified Assets.S3Ops as S3
import qualified Assets.Types as At
import qualified Options.Cli as Cl
import qualified Options.Runtime as Opt

import qualified Assets.Template as Tp

produceCmd :: Cl.ProducerOpts -> Opt.RunOptions -> IO ()
produceCmd prodOpts rtOpts = do
  putStrLn $ "Ingesting product: " <> T.unpack prodOpts.productIG
  putStrLn $ "Version: " <> show prodOpts.versionIG
  case S3.makeS3Conn <$> rtOpts.s3store of
    Nothing -> putStrLn "No S3 connection found"
    Just s3Conn -> do
      runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool -> 
        let
          ctxt = Tp.Context {
            pgPoolCT = pgPool
          , s3RepoCT = storeS3 s3Conn
          }
        in do
        rezA <- Tp.ingestTemplate ctxt prodOpts.templateIG prodOpts.sourceIG prodOpts.productIG
        case rezA of
          Left err -> putStrLn $ "@[produceCmd] ingestTemplate err: " <> show err
          Right productionId -> putStrLn $ "@[produceCmd] productionId: " <> show productionId
      pure ()


storeS3 :: At.S3Conn -> Bs.ByteString -> IO UUID
storeS3 s3Conn bytes = do
  newUUID <- nextRandom
  S3.putFromText s3Conn (T.pack . toString $ newUUID) bytes Nothing
  pure newUUID
