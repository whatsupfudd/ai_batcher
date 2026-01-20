{-# LANGUAGE QuasiQuotes #-}

module Assets.Storage where

import qualified Data.ByteString.Lazy as Lbs
import qualified Data.ByteString as Bs
import Data.Int (Int32, Int64)
import Data.Text (Text, pack)
import qualified Data.Text as T
import qualified Data.UUID as Uu
import qualified Data.UUID.V4 as Uu
import qualified Data.Text.Encoding as T

import qualified Data.ByteString.Base64 as B64

import Hasql.Session (Session, statement)
import Hasql.Pool (Pool, use)
import qualified Hasql.TH as TH

import qualified Network.HTTP.Client.Conduit as Hcc
import qualified Network.HTTP.Types.Status as Hs

import qualified Assets.S3Ops as Ops
import Assets.Types


{-
create table if not exists Asset (
  uid serial primary key
  , eid uuid not null default uuid_generate_v4()
  , created_at timestamp not null default now()
  , status int not null default 1     -- 1: active, 2: disabled, 3: retired.
  , name text
  , description text
  , contentType varchar(255)
  , size bigint not null
  , version int not null default 1
  , notes text
);
-}

prepareNewAsset :: Uu.UUID -> Text -> Text -> Asset
prepareNewAsset eid notes contentType = Asset {
  name = Nothing
  , uid = Nothing
  , eid = eid
  , description = Nothing
  , contentType = contentType
  , size = 0
  , version = 1
  , notes = Just notes
  }


insertNewAsset :: Pool -> S3Conn -> Hcc.Manager -> Hcc.Request -> Asset -> IO (Either String Asset)
insertNewAsset pgPool s3Conn manager request asset = do
  rezA <- Ops.streamHttpResponseToS3 s3Conn manager request (pack . Uu.toString $ asset.eid)
  case rezA of
    Left err ->
      pure . Left $ "@[insertNewAsset] streamHttpResponseToS3 err: " <> show err
    Right aSize -> do
      rezB <- use pgPool $ statement (asset.name, asset.eid, asset.description, asset.contentType, aSize, asset.notes) [TH.singletonStatement|
        insert into Asset
          (name, eid, description, contentType, size, notes)
        values
          ($1::text?, $2::uuid, $3::text?, $4::text, $5::bigint, $6::text?)
        returning uid::int4
      |]
      case rezB of
        Left err ->
          pure . Left $ "@[insertAsset] err: " <> show err
        Right anID ->
          pure . Right $ asset { uid = Just anID, size = aSize }


insertNewAssetFromB64 :: Pool -> S3Conn -> Text -> Asset -> IO (Either String Asset)
insertNewAssetFromB64 pgPool s3Conn b64Text asset =
  let
    rawText = B64.decodeLenient $ T.encodeUtf8 b64Text
  in do
  rezA <- Ops.putFromText s3Conn (pack . Uu.toString $ asset.eid) rawText Nothing
  case rezA of
    Left err ->
      pure . Left $ "@[insertNewAssetFromText] putStream err: " <> show err
    Right _ ->
      let
        aSize = fromIntegral $ Bs.length rawText
      in do
      rezB <- use pgPool $ statement (asset.name, asset.eid, asset.description, asset.contentType, aSize, asset.notes) [TH.singletonStatement|
        insert into Asset
          (name, eid, description, contentType, size, notes)
        values
          ($1::text?, $2::uuid, $3::text?, $4::text, $5::bigint, $6::text?)
        returning uid::int4
      |]
      case rezB of
        Left err ->
          pure . Left $ "@[insertAsset] err: " <> show err
        Right anID ->
          pure . Right $ asset { uid = Just anID, size = aSize }


getAsset :: Pool -> S3Conn -> Uu.UUID -> IO (Either String (Text, Lbs.ByteString))
getAsset pgPool s3Conn assetId = do
  rezA <- use pgPool $ statement assetId [TH.singletonStatement|
    select contentType::text, name::text?, description::text?, size::int8, notes::text?
    from Asset
    where eid = $1::uuid
    order by version desc
    limit 1
  |]
  case rezA of
    Left err ->
      pure . Left $ "@[getAsset] err: " <> show err
    Right (contentType, mbName, mbDescription, size, mbNotes) -> do
      rezB <- Ops.getStream s3Conn (pack . Uu.toString $ assetId)
      case rezB of
        Left err ->
          pure . Left $ "@[getAsset] getStream err: " <> show err
        Right lbs ->
          pure $ Right (contentType, lbs)
