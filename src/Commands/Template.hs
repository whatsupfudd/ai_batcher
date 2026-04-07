module Commands.Template (templateCmd) where

import Control.Monad.Cont (runContT, ContT (..))
import Control.Monad (forM_)

import qualified Data.ByteString.Lazy as Lbs
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (UTCTime, formatTime, defaultTimeLocale)
import Data.UUID (UUID)
import qualified Data.UUID as Uu

import Hasql.Pool (Pool, use)
import qualified Hasql.Session as Hs

import qualified DB.TemplateStmt as Ts
import qualified Assets.S3Ops as S3
import qualified Assets.Storage as St
import qualified DB.Connect as Db
import qualified Options.Cli as Cl
import qualified Options.Runtime as Opt

templateCmd :: Cl.TemplateSubCmds -> Opt.RunOptions -> IO ()
templateCmd subCmd rtOpts = do
  case subCmd of
    Cl.LoadTP loadTO -> doLoad loadTO rtOpts
    Cl.ListTP listTO -> doList listTO rtOpts
    Cl.DeleteTP deleteTO -> doDelete deleteTO rtOpts


doLoad :: Cl.LoadTO -> Opt.RunOptions -> IO ()
doLoad loadTO rtOpts = do
  runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool -> do
    case S3.makeS3Conn <$> rtOpts.s3store of
      Nothing -> putStrLn $ "@[doLoad] no S3 connection found"
      Just s3Conn -> do
        fileBytes <- Lbs.readFile loadTO.loadTemplateFileName
        case TE.decodeUtf8' $ Lbs.toStrict fileBytes of
          Left err -> putStrLn $ "@[doLoad] decodeUtf8' err: " <> show err
          Right fileText ->
            let
              tplSize = fromIntegral (T.length fileText)
            in do
            tplLoc <- St.storeS3 s3Conn fileBytes
            eiLoad <- use pgPool $ Hs.statement (tplLoc, "template" :: Text, tplSize) Ts.insertS3Object
            case eiLoad of
              Left err -> putStrLn $ "@[doLoad] insertS3Object err: " <> show err
              Right () -> do
                eiLoad <- use pgPool $ Hs.statement (loadTO.loadTemplateID, tplLoc) Ts.insertTemplate
                case eiLoad of
                  Left err -> putStrLn $ "@[doLoad] insertTemplate err: " <> show err
                  Right templateId -> putStrLn $ "@[doLoad] template ID: " <> Uu.toString templateId <> " s3 locator: " <> Uu.toString tplLoc


doList :: Cl.ListTO -> Opt.RunOptions -> IO ()
doList listTO rtOpts =
  runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool -> do
    eiList <- case listTO.listTemplateFilter of
      Nothing -> use pgPool $ Hs.statement () Ts.listAllTemplates
      Just filter -> use pgPool $ Hs.statement filter Ts.listTemplates
    case eiList of
      Left err -> putStrLn $ "@[doList] listTemplates err: " <> show err
      Right list -> do
        putStrLn "@[doList] Templates: "
        putStrLn "  Name\t\tCreatedAt\t\t\t\tID\t\t\t\tS3 Locator"
        putStrLn "--------------------------------"
        forM_ list showTemplateInfo

showTemplateInfo :: (Text, UUID, UUID, UTCTime) -> IO ()
showTemplateInfo (name, tid, locator, createdAt) = do
  putStrLn $ "  " <> T.unpack name <> "\t\t" <> formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" createdAt <> "\t\t" <> Uu.toString tid <> "\t\t" <> Uu.toString locator


doDelete :: Cl.DeleteTO -> Opt.RunOptions -> IO ()
doDelete deleteTO rtOpts =
  case Uu.fromString (T.unpack deleteTO.deleteTemplateID) of
    Nothing -> putStrLn $ "@[doDelete] invalid template ID: " <> T.unpack deleteTO.deleteTemplateID
    Just tid -> do
      runContT (Db.startPg rtOpts.pgDbConf) $ \pgPool -> do
        tplLoc <- use pgPool $ Hs.statement tid Ts.fetchTemplateLocator
        case tplLoc of
          Left err -> putStrLn $ "@[doDelete] getTemplateLocator err: " <> show err
          Right tplLoc ->
            case S3.makeS3Conn <$> rtOpts.s3store of
              Nothing -> putStrLn $ "@[doDelete] no S3 connection found"
              Just s3Conn ->
                let
                  tLocator = T.pack . Uu.toString $ tplLoc
                in do
                eiDelete <- use pgPool $ Hs.statement tid Ts.deleteTemplate
                case eiDelete of
                  Left err -> putStrLn $ "@[doDelete] db deleteTemplate err: " <> show err
                  Right () -> do
                    eiDeleteFile <- S3.deleteFile s3Conn tLocator
                    case eiDeleteFile of
                      Left err -> putStrLn $ "@[doDelete] s3 deleteFile err: " <> show err
                      Right () -> putStrLn "@[doDelete] done."

