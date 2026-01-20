{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase #-}

module DB.Opers where

import Control.Monad.IO.Class (liftIO)

import Data.Int (Int16, Int32, Int64)
import Data.Maybe (isJust, fromMaybe)
import Data.Text (Text, unpack, pack)
import Data.Time (UTCTime, NominalDiffTime, addUTCTime, getCurrentTime)
import Data.Vector (Vector)
import qualified Data.Vector as Vc
import Data.UUID (UUID)

import GHC.Generics (Generic)

import Hasql.Session (Session, statement)
import Hasql.Pool (Pool, use)
import qualified Hasql.TH as TH

import qualified Data.Aeson as Ae

import Api.Types (ClientInfo (..))

import qualified Api.RequestTypes as Rq
import qualified Api.ResponseTypes as Rr


checkValidClient :: Pool -> ClientInfo -> IO (Either String Bool)
checkValidClient dbPool clientInfo = do
  rezA <- use dbPool $ statement clientInfo.uid [TH.maybeStatement|
    select 1::int4, count(b.reason)::int4
    from cconnection a
    left join ccancel b on a.uid = b.connection_fk
    where a.uid = $1::int4
  |]
  case rezA of
    Left err -> do
      pure . Left $ "@[checkValidClient] err: " <> show err
    Right (Just (1, 0)) ->
      pure . Right $ True
    Right _ ->
      pure . Right $ False


authUserFromNameSecret :: Pool -> (Text, Text) -> IO (Either String (Maybe (Int32, UUID)))
authUserFromNameSecret dbPool (name, secret) = do
  rezA <- use dbPool $ statement (name, secret) [TH.maybeStatement|
    select a.uid::int4, a.eid::uuid
    from account a
    join authentication b on b.account_fk = a.uid
    where a.name = $1::text and b.method = 1 and b.secret = $2::text
  |]
  case rezA of
    Left err -> do
      pure . Left $ "@[authUserFromNameSecret] select err: " <> show err
    Right (Just ids) -> do
      pure . Right $ Just ids
    Right Nothing ->
      pure . Right $ Nothing


startNewConnection :: Pool -> Int32 -> Text -> IO (Either String Int32)
startNewConnection dbPool accountID token = do
  rezA <- use dbPool $ statement (accountID, token) [TH.singletonStatement|
    insert into cconnection (account_fk, token) values ($1::int4, $2::text)
    returning uid::int4
  |]
  case rezA of
    Left err -> do
      pure . Left $ "@[startNewConnection] err: " <> show err
    Right uid ->
      pure . Right $ uid


serializeInvocation :: Pool -> Int32 -> Rq.InvokeRequest -> Maybe Ae.Value -> IO (Either String Rr.InvokeResponse)
serializeInvocation pgPool accountID request mbResult = do
  rezA <- use pgPool $ statement (accountID, request.function, request.parameters) [TH.singletonStatement|
    insert into crequest (account_fk, function_eid, params) values ($1::int4, $2::uuid, $3::jsonb)
    returning uid::int4, eid::uuid
  |]
  case rezA of
    Left err ->
      pure . Left $ "@[serializeInvocation] err: " <> show err
    Right (uid, eid) ->
      pure . Right $ Rr.InvokeResponse {
        requestID = uid
        , requestEId = eid
        , contextID = 0
        , contextEId = eid
        , status = "OK"
        , result = fromMaybe Ae.Null mbResult
      }


data TransactionStatus =
    CompletedTS
  | FailedTS Text


endTransaction :: Pool -> Rr.InvokeResponse -> TransactionStatus -> IO (Either String Int32)
endTransaction pgPool invResp status =
  let
    (kind, mbNote) = case status of
      CompletedTS -> (1, Nothing)
      FailedTS errMsg -> (5, Just errMsg)
  in do
  rezA <- use pgPool $ statement (invResp.requestID, kind ,mbNote) [TH.singletonStatement|
      insert into ReqExec (crequest_fk, kind, notes) values ($1::int4, $2::int4, $3::text?)
      returning uid::int4
    |]
  case rezA of
    Left err ->
      pure . Left $ "@[endTransaction] err: " <> show err
    Right anID ->
      pure $ Right anID

getResponse :: Pool -> UUID -> IO (Either String Rr.ResponseResponse)
getResponse pgPool requestEID = do
  rezA <- use pgPool $ statement requestEID [TH.maybeStatement|
    select
      a.uid::int4, a.eid::uuid, a.kind::int4, a.content::text?, c.eid::uuid?, c.size::int8?
    from cresponse a
     join crequest d on a.crequest_fk = d.uid
     left join assetresponselink b on a.uid = b.response_fk
     left join asset c on b.asset_fk = c.uid
    where d.eid = $1::uuid
  |]
  case rezA of
    Left err ->
      pure . Left $ "@[getResponse] err: " <> show err
    Right Nothing -> do
      rezB <- use pgPool $ statement requestEID [TH.vectorStatement|
          select
            e.created_at::timestamp, e.kind::text, e.notes::text?
          from crequest d
          join reqexec e on e.crequest_fk = d.uid
          where d.eid = $1::uuid and e.kind <> 1
        |]
      case rezB of
        Left err ->
          pure . Left $ "@[getResponse] err: " <> show err
        Right execs ->
          let
            respResult = if Vc.null execs then
              Rr.NoResponseYetRK
            else
              Rr.AbortedRK (pack $ show execs)
          in
          pure . Right $ Rr.ResponseResponse {
            responseEId = requestEID
            , result = respResult
          }
    Right (Just (uid, eid, kind, mbContent, mbAssetEid, mbAssetSize)) ->
      let
        responseContent = if kind `elem` [1,2,3,4] then
            case mbContent of
              Nothing -> Rr.NoResponseYetRK
              Just content ->
                let
                  tBlockFormat = Rr.PlainTextTF
                in
                Rr.TextBlockRK (Rr.TextBlockRV tBlockFormat content)
          else
            case mbAssetEid of
              -- TODO: manage this better, the response should exist only after the asset has been created
              Nothing -> Rr.NoResponseYetRK
              Just assetEid ->
                Rr.AssetRK (Rr.AssetRV mbContent mbAssetSize assetEid)
      in
      pure . Right $ Rr.ResponseResponse {
        responseEId = eid
        , result = responseContent
      }