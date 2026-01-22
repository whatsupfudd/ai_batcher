{-# LANGUAGE LambdaCase #-}
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


submitBatch :: ServiceConfig ->Manager -> String -> NE.NonEmpty (UUID, Text) -> IO (Either String (Text, [(UUID, Text)]))
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
    Right fileID -> do
      rezA <- createBatch manager apiKey fileID "/v1/responses" "24h"
      case rezA of
        Left err -> pure . Left $ "Failed to create batch: " <> err
        Right submitID -> pure $ Right (submitID, [(batchID, fileID)])


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


uploadFileFromBS :: Manager -> String -> LBS.ByteString -> String -> Text -> IO (Either String Text)
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
  putStrLn $ "Request: " <> show req
  putStrLn $ "Parts: " <> show content
  -- pure $ Left "@[uploadFileFromBS] stopped."
  resp <- httpLbs req manager
  case A.eitherDecode (responseBody resp) of
    Left e  -> pure . Left $ "Files API decode error: " <> e
    Right v -> case v of
      A.Object o -> case At.parseMaybe (A..: "id") o of
        Just (A.String fid) -> pure $ Right fid
        _ -> do
          putStrLn "Response: " >> print v
          pure . Left $ "@[uploadFile] no 'id' in response"
      _ -> pure . Left $ "@[uploadFile] unexpected response"


-- Create a batch from an uploaded JSONL file
createBatch :: Manager -> String -> Text -> Text -> Text -> IO (Either String Text)
createBatch manager apiKey inputFileId endpointPath completionWindow = do
  baseReq <- parseRequest "https://api.openai.com/v1/batches"
  let payload = A.object [
           "input_file_id" .= inputFileId
        , "endpoint" .= endpointPath
        , "completion_window" .= completionWindow
        ]
      req = baseReq {
          method = "POST"
        , requestHeaders = ("Content-Type", "application/json") : authHeaders apiKey
        , requestBody = RequestBodyLBS (A.encode payload)
        }
  putStrLn $ "Request: " <> show req
  -- pure $ Left "@[createBatch] stopped."
  resp <- httpLbs req manager
  case A.eitherDecode (responseBody resp) of
    Left e  -> pure . Left $ "@[createBatch] decode error: " <> e
    Right v -> case v of
      A.Object o -> case At.parseMaybe (A..: "id") o of
        Just (A.String batchID) -> pure $ Right batchID
        _ -> do
          putStrLn "Response: " >> print v
          pure . Left $ "@[createBatch] no 'id' in response"
      _ -> pure . Left $ "@[createBatch] unexpected response"


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

