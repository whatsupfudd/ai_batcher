module Options  (
  module Cl
  , module Fo
  , module Rt
  , mergeOptions
 )
where

import Control.Monad.State ( MonadState (put), MonadIO, runStateT, State, StateT, modify, lift, liftIO )
import Control.Monad.Except ( ExceptT, MonadError (throwError) )
import Data.Functor.Identity ( Identity (..) )

import Data.Foldable (for_)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified System.IO.Error as Serr
import qualified Control.Exception as Cexc
import qualified System.Posix.Env as Senv
import qualified System.Directory as Sdir

import qualified HttpSup.CorsPolicy as Hcrs

import qualified Options.Cli as Cl (CliOptions (..), EnvOptions (..))
import qualified Options.ConfFile as Fo
import qualified Options.Runtime as Rt (RunOptions (..), defaultRun, PgDbConfig (..), defaultPgDbConf)
import qualified Assets.Types as At (S3Config (..), defaultS3Conf)

type ConfError = Either String ()
type RunOptSt = State Rt.RunOptions ConfError
type RunOptIOSt = StateT Rt.RunOptions IO ConfError
type PgDbOptIOSt = StateT Rt.PgDbConfig (StateT Rt.RunOptions IO) ConfError
type S3OptIOSt = StateT At.S3Config (StateT Rt.RunOptions IO) ConfError

mconf :: MonadState s m => Maybe t -> (t -> s -> s) -> m ()
mconf mbOpt setter =
  case mbOpt of
    Nothing -> pure ()
    Just opt -> modify $ setter opt

innerConf :: MonadState s f => (t1 -> s -> s) -> (t2 -> StateT t1 f (Either a b)) -> t1 -> Maybe t2 -> f ()
innerConf updState innerParser defaultVal mbOpt =
  case mbOpt of
    Nothing -> pure ()
    Just anOpt -> do
      (result, updConf) <- runStateT (innerParser anOpt) defaultVal
      case result of
        Left errMsg -> pure ()
        Right _ -> modify $ updState updConf


mergeOptions :: Cl.CliOptions -> Fo.FileOptions -> Cl.EnvOptions -> IO Rt.RunOptions
mergeOptions cli file env = do
  appHome <- case env.appHome of
    Nothing -> do
      eiHomeDir <- Cexc.try Sdir.getHomeDirectory :: IO (Either Serr.IOError FilePath)
      case eiHomeDir of
        Left err -> pure ".fudd/docproc"
        Right aVal -> pure $ aVal <> "/.fudd/docproc"
    Just aVal -> pure aVal
  (result, runtimeOpts) <- runStateT (parseOptions cli file) (Rt.defaultRun appHome "http://localhost" 8886)
  case result of
    Left errMsg -> error errMsg
    Right _ -> pure runtimeOpts
  where
  parseOptions :: Cl.CliOptions -> Fo.FileOptions -> RunOptIOSt
  parseOptions cli file = do
    mconf cli.debug $ \nVal s -> s { Rt.debug = nVal }
    for_ file.server parseServer
    innerConf (\nVal s -> s { Rt.pgDbConf = nVal }) parsePgDb Rt.defaultPgDbConf file.db
    for_ file.jwt parseJWT
    for_ file.cors parseCors
    innerConf (\nVal s -> s { Rt.s3store = Just nVal }) parseS3 At.defaultS3Conf file.s3store
    pure $ Right ()


  parsePgDb :: Fo.PgDbOpts -> PgDbOptIOSt
  parsePgDb dbO = do
    mconf dbO.host $ \nVal s -> s { Rt.host = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.port $ \nVal s -> s { Rt.port = fromIntegral nVal }
    mconf dbO.user $ \nVal s -> s { Rt.user = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.passwd $ \nVal s -> s { Rt.passwd = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.dbase $ \nVal s -> s { Rt.dbase = T.encodeUtf8 . T.pack $ nVal }
    pure $ Right ()


  parseServer :: Fo.ServerOpts -> RunOptIOSt
  parseServer so = do
    mconf so.port $ \nVal s -> s { Rt.serverPort = nVal }
    mconf so.host $ \nVal s -> s { Rt.serverHost = nVal }
    pure $ Right ()

  parseJWT :: Fo.JwtOpts -> RunOptIOSt
  parseJWT jo = do
    case jo.jEnabled of
      Just False -> do
        modify $ \s -> s { Rt.jwkConfFile = Nothing }
        pure $ Right ()
      _ ->
        resolveValue jo.keyFile $ \nVal s -> s { Rt.jwkConfFile = Just nVal }


  parseCors :: Fo.CorsOpts -> RunOptIOSt
  parseCors co = do
    case co.oEnabled of
      Just False ->
        modify $ \s -> s { Rt.corsPolicy = Nothing }
      _ ->
        mconf co.allowed $ \nVal s -> s { Rt.corsPolicy = Just $ Hcrs.defaultCorsPolicy { Hcrs.allowedOrigins = map T.pack nVal } }
    pure $ Right ()

  parseS3 :: Fo.S3Options -> S3OptIOSt
  parseS3 s3O = do
    mconf s3O.accessKey $ \nVal s -> s { At.user = nVal }
    mconf s3O.secretKey $ \nVal s -> s { At.passwd = nVal }
    mconf s3O.host $ \nVal s -> s { At.host = nVal }
    mconf s3O.region $ \nVal s -> s { At.region = nVal }
    mconf s3O.bucket $ \nVal s -> s { At.bucket = nVal }
    pure $ Right ()


-- | resolveEnvValue resolves an environment variable value.
resolveEnvValue :: FilePath -> IO (Maybe FilePath)
resolveEnvValue aVal =
  case head aVal of
      '$' ->
        let
          (envName, leftOver) = break ('/' ==) aVal
        in do
        mbEnvValue <- Senv.getEnv $ tail envName
        case mbEnvValue of
          Nothing -> pure Nothing
          Just aVal -> pure . Just $ aVal <> leftOver
      _ -> pure $ Just aVal

resolveValue :: (MonadIO f, MonadState s f) => Maybe String -> (String -> s -> s) -> f ConfError
resolveValue aVal setter = do
  case aVal of
      Nothing -> pure $ Right ()
      Just aVal -> do
        mbRezVal <- liftIO $ resolveEnvValue aVal
        case mbRezVal of
          Nothing -> pure . Left $ "Could not resolve value: " <> aVal
          Just aVal -> do
            modify $ setter aVal
            pure $ Right ()
