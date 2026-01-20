{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveAnyClass #-}

module Api.Routing.ClientHdl where

import Control.Monad.Except (throwError)
import Control.Monad.IO.Class (liftIO)
import qualified Control.Monad.Reader as Gm

import qualified Data.ByteString.Lazy as Lbs
import Data.Maybe (fromMaybe)
import Data.UUID (UUID)
import qualified Data.UUID as Uu
import Data.ByteString (ByteString)
import Data.Text (Text, unpack, pack)

import GHC.Generics (Generic)

import qualified Data.Aeson as Ae

import Servant.API.Generic
import Servant.API (JSON, PlainText, ReqBody, Get, Post, Delete, OctetStream, (:>), Capture, NoContent, Header, Raw, addHeader, Headers)
import Servant.Auth.Server (Auth, JWT, BasicAuth, AuthResult (..))
import Servant.Server.Generic (AsServerT, genericServerT)

import Api.Types (ClientInfo, AIServerApp, ApiError (..), AppEnv (..), fakeClientInfo, Html (..), SessionItems (..))
import qualified DB.Opers as DbB
import qualified Assets.Storage as DbA
import qualified Api.RequestTypes as Rq
import qualified Api.ResponseTypes as Rr
import qualified Api.Routing.ClientRts as Cr
import qualified Api.Session as Sess


-- Anonymous Handlers:
publicHandlers :: ToServant Cr.PublicRoutes (AsServerT AIServerApp)
publicHandlers = genericServerT $ Cr.PublicRoutes {
    loginPR = loginHandler
    -- , homePage = homePageHandler
  }


loginHandler :: Rq.LoginForm -> AIServerApp Rr.LoginResult
loginHandler form = do
  liftIO $ putStrLn $ "@[loginHandler] user:" <> unpack form.username <> ", pwd: "
      <> unpack form.secret
  -- TODO: check for u/p validity, if so return new Session, otherwise throw error:
  Sess.loginUser form


fromAuthResult :: AuthResult a -> AIServerApp a
fromAuthResult (Authenticated uid) = return uid
fromAuthResult x =
  throwError UnauthorizedAE
    -- . DT.pack . show $ x


privateHandlers :: AuthResult ClientInfo -> ToServant Cr.PrivateRoutes (AsServerT AIServerApp)
privateHandlers authResult = genericServerT $ Cr.PrivateRoutes {
    client = clientHandlers authResult
  , invoke = invokeHandlers authResult
  , asset = assetHandlers authResult
  , retrieve = retrieveHandlers authResult
  , storage = storageHandlers authResult
  }


--- Client:

clientHandlers :: AuthResult ClientInfo -> ToServant Cr.ClientRoutes (AsServerT AIServerApp)
clientHandlers authResult = genericServerT $ Cr.ClientRoutes {
  getClient = getClientHandler authResult
  }


getClientHandler :: AuthResult ClientInfo -> UUID -> AIServerApp ClientInfo
getClientHandler authResult clientId =
  liftIO fakeClientInfo



--- Invoke:
invokeHandlers :: AuthResult ClientInfo -> ToServant Cr.InvokeRoutes (AsServerT AIServerApp)
invokeHandlers authResult = genericServerT $ Cr.InvokeRoutes {
  invokeService = invokeServiceHandler authResult
  }


newtype TestPayload = TestPayload {
    aField :: Text
  }
  deriving (Show, Generic, Ae.ToJSON)
instance Ae.FromJSON TestPayload where
  parseJSON = Ae.withObject "TestPayload" $ \v ->
    TestPayload <$> v Ae..: "aField"

data TestResult = TestResult {
  id :: UUID
  , name :: Text
  , description :: Text
  }
  deriving stock (Show, Generic)
  deriving anyclass Ae.ToJSON

data TestError = TestError {
  code :: Text
  , message :: Text
  }
  deriving stock (Show, Generic)
  deriving anyclass Ae.ToJSON


invokeServiceHandler :: AuthResult ClientInfo -> Rq.InvokeRequest -> AIServerApp Rr.InvokeResponse
invokeServiceHandler (Authenticated clientInfo) request = do
  appEnv <- Gm.ask
  eiValidClient <- liftIO $ DbB.checkValidClient appEnv.pgPool_Ctxt clientInfo
  case eiValidClient of
    Left err ->
      let
        errMsg = "@[invokeServiceHandler] checkValidClient err: " <> err
      in do
        liftIO $ putStrLn errMsg
        throwError . InternalErrorAE $ pack errMsg
    Right checkResult ->
      if checkResult then do
        liftIO $ putStrLn $ "@[invokeServiceHandler] valid client: " <> show clientInfo
        pure $ Rr.InvokeResponse {
          requestID = 0
          , requestEId = Uu.nil
          , contextID = 0
          , contextEId = Uu.nil
          , status = "OK"
          , result = Ae.Null
        }
      else
        let
          errMsg = "@[invokeServiceHandler] invalid client: " <> show clientInfo
        in do
        liftIO $ putStrLn errMsg
        throwError . InternalErrorAE $ pack errMsg


getResponseHandler :: AuthResult ClientInfo -> Maybe UUID -> Maybe Text -> AIServerApp Rr.InvokeResponse
getResponseHandler authResult mbTid mbMode = do
  case mbTid of
    Nothing -> do
      liftIO . putStrLn $ "@[getResponseHandler] tid is missing"
      throwError . InternalErrorAE $ pack "@[getResultHandler] tid is missing"
    Just tid -> do
      appEnv <- Gm.ask
      rezA <- liftIO $ DbB.getResponse appEnv.pgPool_Ctxt tid
      case rezA of
        Left err -> do
          liftIO $ putStrLn $ "@[getResponseHandler] getResponse err: " <> err
          throwError . InternalErrorAE $ pack err
        Right aResponse -> do
          liftIO $ putStrLn $ "@[getResponseHandler] tid: " <> show tid <> " mode: " <> show mbMode
          case aResponse.result of
            Rr.AbortedRK errMsg ->
              pure $ Rr.InvokeResponse {
                requestID = 0
                , requestEId = tid
                , contextID = 0
                , contextEId = tid
                , status = "ABORT"
                , result = Ae.toJSON aResponse
              }
            _ -> 
              pure $ Rr.InvokeResponse {
                requestID = 0
                , requestEId = tid
                , contextID = 0
                , contextEId = tid
                , status = "OK"
                , result = Ae.toJSON aResponse
              }


-- Assets:
assetHandlers :: AuthResult ClientInfo -> ToServant Cr.AssetRoutes (AsServerT AIServerApp)
assetHandlers authResult = genericServerT $ Cr.AssetRoutes {
    getAsset = getAssetHandler authResult
  }

getAssetHandler :: AuthResult ClientInfo -> UUID -> Maybe Text -> AIServerApp (Headers '[Header "Content-Type" Text] ByteString)
getAssetHandler authResult assetId mbP = do
  appEnv <- Gm.ask
  case appEnv.s3Storage_Ctxt of
    Nothing -> do
      liftIO $ putStrLn $ "@[getAssetHandler] s3Storage_Ctxt is not set"
      throwError . InternalErrorAE $ pack "@[getAssetHandler] s3Storage_Ctxt is not set"
    Just s3Conn -> do
      rezA <- liftIO $ DbA.getAsset appEnv.pgPool_Ctxt s3Conn assetId
      case rezA of
        Left err -> do
          liftIO $ putStrLn $ "@[getAssetHandler] getAsset err: " <> err
          throwError . InternalErrorAE $ pack err
        Right (contentType, assetData) -> do
          liftIO $ putStrLn $ "@[getAssetHandler] assetId: " <> show assetId <> " p: " <> show mbP
          pure $ addHeader contentType (Lbs.toStrict assetData)


-- Retrieve:
retrieveHandlers :: AuthResult ClientInfo -> ToServant Cr.RetrieveRoutes (AsServerT AIServerApp)
retrieveHandlers authResult = genericServerT $ Cr.RetrieveRoutes {
    retrieve = retrieveHandler authResult
  }

retrieveHandler :: AuthResult ClientInfo -> UUID -> AIServerApp Rr.InvokeResponse
retrieveHandler authResult requestId = do
  liftIO $ putStrLn $ "@[retrieveHandler] requestId: " <> show requestId 
  liftIO $ Rr.fakeServiceResponse (Ae.toJSON $ TestError "123" "@[retriveHandler] Can't decode payload") "ERROR"


-- Storage:
storageHandlers :: AuthResult ClientInfo -> ToServant Cr.StorageRoutes (AsServerT AIServerApp)
storageHandlers authResult = genericServerT $ Cr.StorageRoutes {
    storagePut = storagePutHandler authResult
  , storageGet = storageGetHandler authResult
  , storageDel = storageDelHandler authResult
  }

storagePutHandler :: AuthResult ClientInfo -> Rq.StoragePut -> AIServerApp Rr.ReplyClientRequest
storagePutHandler authResult storagePut = do
  liftIO $ putStrLn $ "@[storagePutHandler] storagePut: " <> show storagePut 
  pure $ Rr.ReplyClientRequest "OK"

storageGetHandler :: AuthResult ClientInfo -> UUID -> AIServerApp ByteString
storageGetHandler authResult itemId = do
  liftIO $ putStrLn $ "@[storageGetHandler] itemId: " <> show itemId 
  pure "OK"

storageDelHandler :: AuthResult ClientInfo -> UUID -> AIServerApp Rr.ReplyClientRequest
storageDelHandler authResult itemId = do
  liftIO $ putStrLn $ "@[storageDelHandler] itemId: " <> show itemId 
  pure $ Rr.ReplyClientRequest "OK"
