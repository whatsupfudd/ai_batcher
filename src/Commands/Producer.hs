module Commands.Producer (produceCmd) where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- 

import qualified Data.ByteString as Bs
import qualified Data.Text as T
import Data.UUID (UUID)

import qualified DB.Connect as Db
import qualified Assets.S3Ops as S3
import qualified Api.Types as At
import qualified Options.Cli as Cl
import qualified Options.Runtime as Opt

import qualified Assets.Template as Tp

produceCmd :: Cl.ProducerOpts -> Opt.RunOptions -> IO ()
produceCmd prodOpts rtOpts = do
  putStrLn $ "Ingesting product: " <> T.unpack prodOpts.productIG
  putStrLn $ "Version: " <> show prodOpts.versionIG
  runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool -> 
    let
      s3Conn = S3.makeS3Conn <$> rtOpts.s3store
      ctxt = Tp.Context {
        pgPoolCT = pgPool
      , s3RepoCT = storeS3 s3Conn
      }
    in
    Tp.ingestTemplate ctxt prodOpts.templatePath prodOpts.sourcePath prodOpts.productionName

  pure ()

storeS3 :: At.S3Conn -> Bs.ByteString -> IO UUID
storeS3 s3Conn path = do
  bytes <- BS.readFile path
  S3.uploadS3 s3Conn bytes
