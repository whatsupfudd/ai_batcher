module Api.Session where


import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader (asks)
import Control.Monad.Except (runExceptT, withExceptT)

import qualified Data.ByteString.Lazy as Lbs
import Data.Time.Clock (secondsToDiffTime, DiffTime, getCurrentTime, addUTCTime)
import Data.Text (Text, unpack, pack)
import Data.Text.Encoding (decodeUtf8)

import Servant.Auth.Server ( AuthResult (..)
            , BasicAuthData (..)
            , CookieSettings (..), SetCookie (..)
            , JWTSettings (..), ToJWT (..), makeJWT, JWT
            , decodeJWT, verifyJWT, IsMatch (..)
          )

import qualified Data.Aeson as Ae
import qualified Crypto.JOSE as Crj
import qualified Crypto.JWT as Crt

import Hasql.Pool (Pool, use)

import Api.Types (AppEnv (..), AIServerApp (..), ClientInfo (..), SessionItems (..), fakeClientInfo)
import Api.RequestTypes (LoginForm (..))
import Api.ResponseTypes (LoginResult (..))
import Control.Monad.Error.Class (liftEither)
import Control.Arrow (first)

import qualified DB.Opers as DbB


loginUser :: LoginForm -> AIServerApp LoginResult
loginUser request = do
  dbPool <- asks pgPool_Ctxt
  jwtInfo <- asks jwt_Ctxt
  -- TODO: extract the session duration from the config:
  -- eiRez <- authorizeNewSession dbPool jwtInfo request (secondsToDiffTime 86400)
  eiRez <- authorizeNewSession dbPool jwtInfo request (secondsToDiffTime 86400)
  case eiRez of
    Left errMsg -> do
      liftIO $ putStrLn $ "@[loginReq] authorizeNewSession err: " <> errMsg
      pure . ErrorLR $ "@[loginReq] problem with authorizeNewSession."
    Right authRez ->
      pure authRez


authorizeNewSession :: Pool -> JWTSettings -> LoginForm -> DiffTime -> AIServerApp (Either String LoginResult)
authorizeNewSession dbPool jwtInfo lForm sessionDuration = do
  rezA <- liftIO $ DbB.authUserFromNameSecret dbPool (lForm.username, lForm.secret)
  case rezA of
    Left err -> pure . Left $ "@[authorizeNewSession] authUserFromNameSecret err: " <> err
    Right Nothing -> do
      liftIO $ putStrLn $ "@[loginReq] authorizeNewSession no user found: " <> unpack lForm.username <> "."
      pure . Right . UnauthorizedLR $ "@[loginReq] invalid credentials for " <> unpack lForm.username <> "."
    Right (Just (uid, eid)) -> do
      now <- liftIO getCurrentTime
      let
        sessionDuration = addUTCTime (365 * 24 * 60 * 60) now
        clientInfo = ClientInfo uid eid sessionDuration
      rezC <- liftIO $ makeJWT clientInfo jwtInfo Nothing
      case rezC of
        Left err -> pure . Left $ "@[authorizeNewSession] makeJWT err: " <> show err
        Right token -> do
          rezB <- liftIO $ DbB.startNewConnection dbPool uid (decodeUtf8 . Lbs.toStrict $ token)
          case rezB of
            Left err -> pure . Left $ "@[authorizeNewSession] startNewConnection err: " <> err
            Right connID ->
              pure . Right $ AuthenticatedLR (SessionItems clientInfo connID (decodeUtf8 . Lbs.toStrict $ token))


validateUser :: Pool -> JWTSettings -> Lbs.ByteString -> IO (AuthResult ClientInfo)
validateUser dbPool jwtInfo token = do
  liftIO $ putStrLn $ "@[validateUser] token: " <> show token
  eiClientInfo <- jwtDecode jwtInfo token
  case eiClientInfo of
    Left err -> pure Indefinite
    Right clientInfo -> do
      liftIO $ putStrLn $ "@[validateUser] clientInfo: " <> show clientInfo
      pure $ Authenticated clientInfo


jwtDecode :: JWTSettings -> Lbs.ByteString -> IO (Either Text ClientInfo)
jwtDecode jwtInfo token = do
  jwkSet <- validationKeys jwtInfo
  eiVerifiedToken <- runExceptT . withExceptT fromJWTError $ do
      unverifiedJWT <- Crj.decodeCompact token
      Crt.verifyClaims (validSettings jwtInfo) jwkSet unverifiedJWT
  let
    eiRez = eiVerifiedToken >>= decodeJWT
  pure eiRez
  where
  fromJWTError :: Crt.JWTError -> Text
  fromJWTError = pack . show


validSettings :: JWTSettings -> Crt.JWTValidationSettings
validSettings s = Crt.defaultJWTValidationSettings (toBool <$> audienceMatches s)
  where
  toBool Matches = True
  toBool DoesNotMatch = False
