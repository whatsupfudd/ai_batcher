{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
module Service.OpenAI where

import Data.Bits ( xor )
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NE
import Data.Maybe ( fromMaybe )
import Numeric ( showHex )
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (getCurrentTime)
import qualified Data.Time as Tm
import Data.UUID (UUID)
import qualified Data.UUID as Uu
import qualified Data.UUID.V4 as U4
import Data.Word (Word64, Word8)

import GHC.Generics ( Generic )

import qualified Data.Aeson as A
import qualified Data.Aeson.Encode.Pretty as AP
import Data.Aeson ( (.=) )
import qualified Data.Aeson.Types as At

import Network.HTTP.Client ( Request(..), Response(..)
                          , RequestBody(..)
                          , Manager
                          , httpLbs
                          , method
                          , newManager
                          , parseRequest
                          , requestBody
                          , requestHeaders
                          , responseBody
                          )
import Network.HTTP.Client.TLS ( tlsManagerSettings )
import Network.HTTP.Client.MultipartFormData ( partBS, partFileRequestBody, formDataBody )
import Network.HTTP.Types (HeaderName)

import Service.Types


data BatchStatus = BatchStatus
  { idBS              :: Text
  , statusBS          :: Text
  , outputFileIdBS    :: Maybe Text
  , errorFileIdBS     :: Maybe Text
  , requestCountsBS   :: Maybe At.Value  -- keep flexible for now
  } deriving (Show)


submitBatch :: ServiceConfig ->Manager -> String -> NE.NonEmpty (UUID, Text) -> IO (Either String (Text, UUID))
submitBatch cfg manager apiKey requestPairs = do
  moment <- getCurrentTime
  batchID <- U4.nextRandom
  let
    momentStr = Tm.formatTime Tm.defaultTimeLocale "%y%m%d_%H%M%S" moment
    cacheKey = calcKeyFromRequests requestPairs
    systemPrompt = fromMaybe "" cfg.systemPromptSC
    requests = zipWith (curry (\((reqID, userPrompt), idx) ->
          oneLineJSONL idx momentStr systemPrompt userPrompt cacheKey cfg.modelSC cfg.effortSC)) (NE.toList requestPairs) [1..]
    mergedRequests = LBS.fromStrict $ TE.encodeUtf8 $ T.intercalate "\n" requests
  uploadFileFromBS manager apiKey mergedRequests (Uu.toString batchID) "batch" >>= \case
    Left err -> pure . Left $ "Failed to upload batch: " <> err
    Right fileResponse -> do
      rezA <- createBatch manager apiKey batchID "/v1/responses" "24h"
      case rezA of
        Left err -> pure . Left $ "Failed to create batch: " <> err
        Right sResponse -> pure $ Right (sResponse.idSR, batchID)


calcKeyFromRequests :: NE.NonEmpty (UUID, Text) -> Text
calcKeyFromRequests requestPairs =
  let
    requestsText = foldl (\acc (reqID, contentText) -> acc <> "\n" <> contentText) "" requestPairs
  in
  "doc-fnv1a64-" <> (T.pack . toHex64 . fnv1a64 $ TE.encodeUtf8 requestsText)

oneLineJSONL :: Int -> String -> Text -> Text -> Text -> Text -> Text -> Text
oneLineJSONL i moment systemPrompt userPrompt cacheKey model effort =
  let customId  = T.pack ("q_" <> moment <> "_" <> pad4 i)
      body :: A.Value
      body = A.object
        [ "model"            .= model
        , "prompt_cache_key" .= cacheKey
        , "reasoning"        .= A.object ["effort" .= effort]
        , "max_output_tokens" .= A.Number 200000
        , "input"            .= A.toJSON
            [ A.object
                [ "role"    .= ("system" :: Text)
                , "content" .= [ A.object ["type" .= ("input_text" :: Text)
                                           , "text" .= systemPrompt
                                           ]
                               ]
                ]
            , A.object
                [ "role"    .= ("user" :: Text)
                , "content" .= [ A.object ["type"   .= ("input_text" :: Text)
                                           , "text"   .= userPrompt
                                           ]
                               ]
                ]
            ]
        ]
      lineObj = A.object
        [ "custom_id" .= customId
        , "method"    .= ("POST" :: Text)
        , "url"       .= ("/v1/responses" :: Text)
        , "body"      .= body
        ]
  in TE.decodeUtf8 (LBS.toStrict (A.encode lineObj))


data FileUploadResponse = FileUploadResponse {
  idFR :: Text
  , objectFR :: Text
  , bytesFR :: Int
  , createdAtFR :: Int
  , expiresAtFR :: Int
  , filenameFR :: Text
  , purposeFR :: Text
} deriving (Show, Generic)

instance A.FromJSON FileUploadResponse where
  parseJSON = A.withObject "FileUploadResponse" $ \o ->
    FileUploadResponse <$> o A..: "id"
      <*> o A..: "object"
      <*> o A..: "bytes"
      <*> o A..: "created_at"
      <*> o A..: "expires_at"
      <*> o A..: "filename"
      <*> o A..: "purpose"


uploadFileFromBS :: Manager -> String -> LBS.ByteString -> String -> Text -> IO (Either String FileUploadResponse)
uploadFileFromBS manager apiKey content batchID purpose = do
  baseReq <- parseRequest "https://api.openai.com/v1/files"
  let
    parts =  [
          partFileRequestBody "file" (batchID <> ".jsonl") $ RequestBodyLBS content
        , partBS "purpose" (TE.encodeUtf8 purpose)
        ]
    fullRequest = baseReq {
        method = "POST"
      , requestHeaders = authHeaders apiKey
      }
  req <- formDataBody parts fullRequest
  -- putStrLn $ "Request: " <> show req
  -- putStrLn $ "Parts: " <> show content
  -- pure $ Left "@[uploadFileFromBS] stopped."
  resp <- httpLbs req manager
  pure $ A.eitherDecode resp.responseBody


data BatchSubmitResponse = BatchSubmitResponse {
  idSR :: Text
  , statusSR :: Text
  , errorsSR :: Maybe Text
  , outputFileIdSR :: Maybe Text
  , errorFileIdSR :: Maybe Text
  , completedAtSR :: Maybe Int
  , failedAtSR :: Maybe Int
  , expiredAtSR :: Maybe Int
  , cancellingAtSR :: Maybe Int
  , cancelledAtSR :: Maybe Int
  , requestCountsSR :: Maybe RequestCountsSR
  , usageSR :: Maybe UsageSR
} deriving (Show, Generic)

instance A.FromJSON BatchSubmitResponse where
  parseJSON = A.withObject "BatchSubmitResponse" $ \o ->
    BatchSubmitResponse 
      <$> o A..: "id"
      <*> o A..: "status"
      <*> o A..:? "errors"
      <*> o A..:? "output_file_id"
      <*> o A..:? "error_file_id"
      <*> o A..:? "completed_at"
      <*> o A..:? "failed_at"
      <*> o A..:? "expired_at"
      <*> o A..:? "cancelling_at"
      <*> o A..:? "cancelled_at"
      <*> o A..:? "request_counts"
      <*> o A..:? "usage"


data RequestCountsSR = RequestCountsSR {
  totalRC :: Int
  , completedRC :: Int
  , failedRC :: Int
} deriving (Show, Generic)

instance A.FromJSON RequestCountsSR where
  parseJSON = A.withObject "RequestCountsSR" $ \o ->
    RequestCountsSR <$> o A..: "total" <*> o A..: "completed" <*> o A..: "failed"

data UsageSR = UsageSR {
  inputTokensSR :: Int
  , inputTokensDetailsSR :: Maybe InputTokensDetailsSR
  , outputTokensSR :: Int
  , outputTokensDetailsSR :: Maybe OutputTokensDetailsSR
  , totalTokensSR :: Int
} deriving (Show, Generic)

instance A.FromJSON UsageSR where
  parseJSON = A.withObject "UsageSR" $ \o ->
    UsageSR <$> o A..: "input_tokens"
      <*> o A..:? "input_tokens_details"
      <*> o A..: "output_tokens"
      <*> o A..:? "output_tokens_details"
      <*> o A..: "total_tokens"

newtype InputTokensDetailsSR = InputTokensDetailsSR {
  cachedTokensSR :: Int
} deriving (Show, Generic)

instance A.FromJSON InputTokensDetailsSR where
  parseJSON = A.withObject "InputTokensDetailsSR" $ \o ->
    InputTokensDetailsSR <$> o A..: "cached_tokens"

newtype OutputTokensDetailsSR = OutputTokensDetailsSR {
  reasoningTokensSR :: Int
} deriving (Show, Generic)

instance A.FromJSON OutputTokensDetailsSR where
  parseJSON = A.withObject "OutputTokensDetailsSR" $ \o ->
    OutputTokensDetailsSR <$> o A..: "reasoning_tokens"
 

-- Create a batch from an uploaded JSONL file
createBatch :: Manager -> String -> UUID -> Text -> Text -> IO (Either String BatchSubmitResponse)
createBatch manager apiKey batchID endpointPath completionWindow = do
  baseReq <- parseRequest "https://api.openai.com/v1/batches"
  let
    payload = A.object [
           "input_file_id" .= (Uu.toString batchID <> ".jsonl")
        , "endpoint" .= endpointPath
        , "completion_window" .= completionWindow
        ]
    req = baseReq {
        method = "POST"
      , requestHeaders = ("Content-Type", "application/json") : authHeaders apiKey
      , requestBody = RequestBodyLBS (A.encode payload)
      }
  -- putStrLn $ "Request: " <> show req
  -- pure $ Left "@[createBatch] stopped."
  resp <- httpLbs req manager
  pure $ A.eitherDecode resp.responseBody


-- Common auth headers
authHeaders :: String -> [(HeaderName, BS8.ByteString)]
authHeaders key =
  [ ("Authorization", BS8.pack ("Bearer " <> key))
  , ("OpenAI-Beta", "prompt-caching-1") -- harmless if ignored
  ]


pad4 :: Int -> String
pad4 n = let s = show n in replicate (4 - length s) '0' ++ s

-- FNV-1a 64-bit (to derive a stable default prompt_cache_key)
fnv1a64 :: BS.ByteString -> Word64
fnv1a64 = BS.foldl' step offset
  where
    offset :: Word64
    offset = 14695981039346656037
    prime  :: Word64
    prime  = 1099511628211
    step :: Word64 -> Word8 -> Word64
    step h b = (h `xor` fromIntegral b) * prime

-- Hex printer for Word64
toHex64 :: Word64 -> String
toHex64 w = let s = showHex w ""
            in replicate (16 - length s) '0' ++ s

-- Fetching:

getBatchStatus :: Manager -> String -> String -> IO (Either String ProviderBatchStatus)
getBatchStatus manager apiKey bid = do
  req0 <- parseRequest ("https://api.openai.com/v1/batches/" ++ bid)
  let req = req0 { method = "GET", requestHeaders = authHeaders apiKey }
  resp <- httpLbs req manager
  case A.eitherDecode (responseBody resp) >>= At.parseEither parseBatchStatus of
    Left errMsg -> pure . Left $ "Failed to parse batch status: " <> errMsg
    Right bStatus -> case bStatus.statusBS of
      "completed" -> pure . Right $ BatchCompleted
      "failed" -> pure . Right $ BatchFailed (fromMaybe "" bStatus.errorFileIdBS)
      "cancelled" -> pure . Right $ BatchCancelled (fromMaybe "" bStatus.errorFileIdBS)
      _ -> pure . Left $ "Unknown batch status: " <> T.unpack bStatus.statusBS

getFileContent :: Manager -> String -> String -> IO LBS.ByteString
getFileContent manager apiKey fid = do
  req0 <- parseRequest ("https://api.openai.com/v1/files/" ++ fid ++ "/content")
  let req = req0 { method = "GET", requestHeaders = authHeaders apiKey }
  resp <- httpLbs req manager
  pure (responseBody resp)


parseBatchStatus :: A.Value -> At.Parser BatchStatus
parseBatchStatus = A.withObject "Batch" $ \o -> do
  idN <- o A..:  "id"
  statusN <- o A..:  "status"
  ouptputFileIdN <- o A..:? "output_file_id"
  errorFileIdN  <- o A..:? "error_file_id"
  requestCountsN <- o A..:? "request_counts"
  pure $ BatchStatus idN statusN ouptputFileIdN errorFileIdN requestCountsN

