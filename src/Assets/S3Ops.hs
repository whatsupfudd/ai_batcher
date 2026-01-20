module Assets.S3Ops where

import Data.Conduit (ConduitM, ConduitT, runConduitRes, yield)
import Data.Conduit.Binary (sinkLbs)
import qualified Control.Monad.Reader as Cmr
import Control.Monad.IO.Class (liftIO)
import qualified Control.Exception as Cexc


import qualified Data.ByteString.Lazy as Lbs
import qualified Data.ByteString as Bs
import Data.Int (Int64)
import qualified Data.List as L
import Data.String (fromString)
import Data.Text (Text, unpack, pack)
import qualified Data.Vector as Vc

import Conduit (runConduit, liftIO, (.|), sinkList, sinkFile, foldlC)
import UnliftIO (throwIO, try)

import qualified Network.HTTP.Client as Hc
import qualified Network.HTTP.Client.Conduit as Hcc
import qualified Network.HTTP.Client.TLS as Hct
import qualified Network.HTTP.Types.Header as Hh
import qualified Network.HTTP.Types.Status as Hs

import qualified Network.Minio as Mn

import Assets.Types
import qualified Data.Text.Encoding as T


makeS3Conn :: S3Config -> S3Conn
makeS3Conn conf =
  let
    creds = Mn.CredentialValue (Mn.AccessKey conf.user) (fromString $ unpack conf.passwd) Nothing
    ciHost :: Mn.ConnectInfo
    ciHost = fromString $ unpack conf.host
    connInfo = Mn.setRegion conf.region $ Mn.setCreds creds ciHost
  in
  S3Conn {
    bucketCn = conf.bucket
    , credentialsCn = creds
    , connInfoCn = connInfo
  }


streamHttpResponseToS3 :: S3Conn -> Hc.Manager -> Hcc.Request -> Text -> IO (Either String Int64)
streamHttpResponseToS3 s3Conn manager request objectKey = do
  rezA <- liftIO . Cexc.try $ Cmr.runReaderT (
    Hcc.withResponse request $ \response -> do
        liftIO $ putStrLn $ "@[streamHttpResponseToS3] responseHeaders: " <> show (Hc.responseHeaders response)
        if Hc.responseStatus response /= Hs.status200
          then
            pure . Left $ "HTTP error: " ++ show (Hc.responseStatus response)
          else do
            let bodyReader = Hcc.responseBody response
                mbLen = L.find (\(k, v) -> k == Hh.hContentLength) (Hc.responseHeaders response)
            -- now call your putStream helper:
            liftIO $ putStream s3Conn objectKey bodyReader Nothing
    ) manager :: IO (Either Cexc.SomeException (Either String ()))
  case rezA of
    Left err -> pure . Left $ "@[streamHttpResponseToS3] http exception: " <> show err
    Right innerRez ->
      case innerRez of
        Left err -> pure . Left $ "@[streamHttpResponseToS3] withResponse/putStream err: " <> show err
        Right () -> do
          eiSize <- Mn.runMinio s3Conn.connInfoCn $ do
              rezA <- Mn.getObject s3Conn.bucketCn objectKey Mn.defaultGetObjectOptions
              let
                objInfo = Mn.gorObjectInfo rezA
              pure (Mn.oiSize objInfo)
          case eiSize of
            Left err -> pure . Left $ "@[streamHttpResponseToS3] getObject err: " <> show err
            Right size -> pure $ Right size


putStream :: S3Conn -> Text -> ConduitT () Bs.ByteString Mn.Minio () -> Maybe Int64 -> IO (Either String ())
putStream s3Conf locator sink mbSize = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
    Mn.putObject s3Conf.bucketCn locator sink mbSize Mn.defaultPutObjectOptions
  case res of
    Left e -> pure . Left $ "@[putStream] file upload failed due to " ++ show e
    Right () -> pure $ Right ()


putFromText :: S3Conn -> Text -> Bs.ByteString -> Maybe Int64 -> IO (Either String ())
putFromText s3Conf locator textData mbSize = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
    Mn.putObject s3Conf.bucketCn locator (yield textData) mbSize Mn.defaultPutObjectOptions
  case res of
    Left e -> pure . Left $ "@[putStream] file upload failed due to " ++ show e
    Right () -> pure $ Right ()


-- TODO: send back upload time.
putFile :: S3Conn -> FilePath -> Text -> IO (Either String ())
putFile s3Conf filePath locator = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      -- Make a bucket; catch bucket already exists exception if thrown.
      bErr <- try $ Mn.makeBucket s3Conf.bucketCn Nothing
      case bErr of
        Left Mn.BucketAlreadyOwnedByYou -> pure ()
        Left e -> throwIO e
        Right _ -> pure ()

      -- Upload filepath to bucket; object is derived from filepath.
      Mn.fPutObject s3Conf.bucketCn locator filePath Mn.defaultPutObjectOptions
  case res of
    Left e -> pure . Left $ "@[putFile] file upload failed due to " ++ show e
    Right () -> pure $ Right ()


getFile :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFile s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      Mn.fGetObject s3Conf.bucketCn locator filePath Mn.defaultGetObjectOptions
  case res of
    Left e -> pure . Left $ "@[getFile] file download failed due to " ++ show e
    Right () -> pure $ Right ()

getFileB :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFileB s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      rezA <- Mn.getObject s3Conf.bucketCn locator Mn.defaultGetObjectOptions
      let
        objInfo = Mn.gorObjectInfo rezA
      liftIO $ putStrLn $ "@[getFileB] size: " <> show (Mn.oiSize objInfo) <> ", modTime: " <> show (Mn.oiModTime objInfo)
      runConduit $ Mn.gorObjectStream rezA .| sinkFile filePath

  case res of
    Left e -> pure . Left $ "@[getFileB] file download failed due to " ++ show e
    Right () -> pure $ Right ()

getStream :: S3Conn -> Text -> IO (Either String Lbs.ByteString)
getStream s3Conf locator = do
  rezA <-Mn.runMinio s3Conf.connInfoCn $ do
      objData <- Mn.getObject s3Conf.bucketCn locator Mn.defaultGetObjectOptions
      let
        objInfo = Mn.gorObjectInfo objData
      -- liftIO $ putStrLn $ "@[getFileB] size: " <> show (Mn.oiSize objInfo) <> ", modTime: " <> show (Mn.oiModTime objInfo)
      runConduit $ Mn.gorObjectStream objData .| sinkLbs
  case rezA of
    Left err -> pure . Left $ "@[getStream] file download failed due to " ++ show err
    Right lbs -> pure $ Right lbs

listFiles :: S3Conn -> Maybe Text -> IO (Either String [FilePath])
listFiles s3Conf path = do
  {- Testing:
  rezT1 <- Mn.runMinio s3Conf.connInfoCn $ do
    Mn.listBuckets
  putStrLn $ "@[listFiles] rezT1: " <> show rezT1
  --}
  -- putStrLn $ "@[listFiles] starting, path: " <> show path <> ", bucket: " <> show s3Conf.bucketCn
  rezA <- Mn.runMinio s3Conf.connInfoCn $ do
    runConduit $ Mn.listObjects s3Conf.bucketCn path False .| sinkList
  -- liftIO $ putStrLn $ show (take 5 rezA)
  -- putStrLn "@[listFiles] done..."
  case rezA of
    Left err -> pure . Left $ show err
    Right listItems -> do
      -- mapM_ print listItems
      -- putStrLn $ "@[listFiles] length: " <> show (length listItems)
      pure . Right $ map (unpack . itemToText) listItems

itemToText anItem =
  case anItem of
    Mn.ListItemPrefix p -> p
    Mn.ListItemObject o -> Mn.oiObject o


listFilesWith :: S3Conn -> Vc.Vector Text  -> IO (Either String [FilePath])
listFilesWith s3Conf paths = do
  mgr <- Hc.newManager Hc.defaultManagerSettings
  conn <- Mn.mkMinioConn s3Conf.connInfoCn mgr
  rezA <- mapM (\aPath -> Mn.runMinioWith conn $ do
      -- foldlC (\accum v -> accum <> v) 0
      runConduit $ Mn.listObjects s3Conf.bucketCn (Just aPath) True .| foldlC (\accum item -> accum <> [unpack $ itemToText item]) []
    ) paths
  -- pure . Right $ foldl (\accum rez -> accum + (case rez of Left err -> 0; Right aList -> Vc.length (Vc.fromList aList))) 0 rezA
  let
    (allFound, errors) = foldl (\(items, errs) rez ->
      case rez of
        Left anErr ->
          let
            errMsg = "e: " <> show anErr
          in
          (items, if errs == "" then errMsg else errs <> ", " <> errMsg)
        Right aList -> (items <> aList, errs)
      ) ([], "") rezA
  case errors of
    "" -> pure $ Right allFound
    aVal -> pure $ Left aVal
