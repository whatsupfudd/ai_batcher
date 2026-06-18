module Commands.Process (processCmd) where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- 
import Control.Concurrent (threadDelay)
import Control.Monad (forM_)

import qualified Data.ByteString.Lazy as Lbs
import Data.Either (lefts, rights)
import Data.Maybe (fromMaybe)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import Data.UUID (UUID, toString)
import Data.UUID.V4 (nextRandom)
import qualified Data.UUID as Uu
import Data.Vector (Vector)

import System.FilePath ((</>))

import qualified Data.Aeson as Ae

import Network.HTTP.Client.TLS ( tlsManagerSettings )
import Network.HTTP.Client as Hc

import qualified Hasql.Pool as Pool

import qualified DB.Connect as Db
import qualified Assets.S3Ops as S3
import qualified Assets.Storage as St
import qualified Options.Cli as Cl
import qualified Assets.Types as At
import qualified Assets.Template as Tp
import qualified Options.Runtime as Opt
import qualified PostProc.Access as Pa
import qualified PostProc.Convert as Pc


processCmd :: Cl.ProcessOpts -> Opt.RunOptions -> IO ()
processCmd processOpts rtOpts = do
  case S3.makeS3Conn <$> rtOpts.s3store of
    Nothing -> putStrLn "No S3 connection found"
    Just s3Conn -> do
      runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool ->
        let
          ctxt = Tp.Context {
              pgPoolCT = pgPool
            , s3RepoCT = St.storeS3 s3Conn
            , s3FetchCT = St.fetchTemplateFromS3 s3Conn
            }
        in do
          postProcessResults pgPool processOpts
      pure ()


postProcessResults :: Pool.Pool -> Cl.ProcessOpts -> IO (Either String ())
postProcessResults pgPool procOpts = do
  putStrLn $ "@[postProcessResults] productionName: " <> show procOpts.productionNamePC
  results <- Pa.fetchResultsForProduction pgPool procOpts.productionNamePC
  case results of
    Left errMsg -> do
      putStrLn $ "@[postProcessResults] error: " <> errMsg
      pure $ Left errMsg
    Right results -> do
      case procOpts.outputModePC of
        "docx" -> do
          forM_ results $ \(idx, createdAt, content) ->
            let
              docName = T.unpack procOpts.productionNamePC <> "_" <> show idx
            in do
            putStrLn $ "@[postProcessResults] docx file: " <> show idx
            Pc.convToDocx (fromMaybe "/tmp" procOpts.outputDirPC) docName content
          pure $ Right ()
        "htmljs" -> do
          forM_ results $ \(idx, createdAt, content) -> do
            putStrLn $ "@[postProcessResults] htmljs on: " <> show idx
            Pc.convToHtmlJS (fromMaybe "/tmp" procOpts.outputDirPC) content
          pure $ Right ()
        "code" -> do
          forM_ results $ \(idx, createdAt, content) -> do
            putStrLn $ "@[postProcessResults] code on: " <> show idx
            Pc.convToCode (fromMaybe "/tmp" procOpts.outputDirPC) content
          pure $ Right ()
        "text" -> do
          forM_ results $ \(idx, createdAt, content) ->
            let
              docName = T.unpack procOpts.productionNamePC <> "_" <> show idx <> ".txt"
              fullName = fromMaybe "/tmp" procOpts.outputDirPC </> docName
            in do
            putStrLn $ "@[postProcessResults] text file: " <> fullName
            TIO.writeFile fullName content
          pure $ Right ()
        _ -> do
          putStrLn $ "@[postProcessResults] invalid output mode: " <> show procOpts.outputModePC
          pure $ Left "Invalid output mode"
