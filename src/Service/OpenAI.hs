{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
module Service.OpenAI where

import Data.Bits ( xor )
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NE
import Data.Maybe ( fromMaybe, maybeToList )
import Numeric ( showHex )
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (getCurrentTime)
import qualified Data.Time as Tm
import Data.UUID (UUID)
import qualified Data.UUID as Uu
import qualified Data.UUID.V4 as U4
import qualified Data.Vector as V
import Data.Word (Word64, Word8)

import GHC.Generics ( Generic )

import qualified Data.Aeson as A
import qualified Data.Aeson.Encode.Pretty as AP
import Data.Aeson ((.=), (.:?), (.:), (.:?))
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
          oneLineJSONL reqID idx momentStr systemPrompt userPrompt cacheKey cfg.modelSC cfg.effortSC)) (NE.toList requestPairs) [1..]
    mergedRequests = LBS.fromStrict $ TE.encodeUtf8 $ T.intercalate "\n" requests
  uploadFileFromBS manager apiKey mergedRequests (Uu.toString batchID) "batch" >>= \case
    Left err -> pure . Left $ "@[submitBatch] uploadFileFromBS err: " <> err
    Right fileResponse -> do
      rezA <- createBatch manager apiKey fileResponse.idFR "/v1/responses" "24h"
      case rezA of
        Left err -> pure . Left $ "@[submitBatch] createBatch err: " <> err
        Right sResponse -> pure $ Right (sResponse.idSR, batchID)


calcKeyFromRequests :: NE.NonEmpty (UUID, Text) -> Text
calcKeyFromRequests requestPairs =
  let
    requestsText = foldl (\acc (reqID, contentText) -> acc <> "\n" <> contentText) "" requestPairs
  in
  "doc-fnv1a64-" <> (T.pack . toHex64 . fnv1a64 $ TE.encodeUtf8 requestsText)

oneLineJSONL :: UUID -> Int -> String -> Text -> Text -> Text -> Text -> Maybe Text -> Text
oneLineJSONL reqID i moment systemPrompt userPrompt cacheKey model mbEffort =
  let
    customId  = Uu.toString reqID    -- T.pack ("q_" <> moment <> "_" <> pad4 i)
    body :: A.Value
    body = A.object $
      [ "model"            .= model
      , "prompt_cache_key" .= cacheKey
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
      <> case mbEffort of
        Just effort -> [ "reasoning" .= A.object ["effort" .= effort] ]
        Nothing -> []
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
  -- putStrLn $ "@[uploadFileFromBS] response: " <> show resp.responseBody
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
createBatch :: Manager -> String -> Text -> Text -> Text -> IO (Either String BatchSubmitResponse)
createBatch manager apiKey fileID endpointPath completionWindow = do
  baseReq <- parseRequest "https://api.openai.com/v1/batches"
  let
    payload = A.object [
           "input_file_id" .= fileID
        , "endpoint" .= endpointPath
        , "completion_window" .= completionWindow
        ]
    req = baseReq {
        method = "POST"
      , requestHeaders = ("Content-Type", "application/json") : authHeaders apiKey
      , requestBody = RequestBodyLBS (A.encode payload)
      }
  -- putStrLn $ "@[createBatch] request: " <> show (A.encode payload)
  -- pure $ Left "@[createBatch] stopped."
  resp <- httpLbs req manager
  -- putStrLn $ "@[createBatch] response: " <> show resp.responseBody
  case A.eitherDecode resp.responseBody :: Either String BatchSubmitResponse of
    Left err -> pure . Left $ "@[createBatch] decode err: " <> err <> ", response: " <> show resp.responseBody
    Right bResponse -> pure $ Right bResponse


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

getBatchStatus :: Manager -> String -> (UUID, String) -> IO (Either String ProviderBatchStatus)
getBatchStatus manager apiKey (bid, providerBatchId) = do
  innerGetBatchStatus manager apiKey (bid, providerBatchId) >>= \case
    Left (errMsg, respBody) -> pure . Left $ "@[getBatchStatus] innerGetBatchStatus err: " <> errMsg <> ", response: " <> show respBody
    Right bStatus -> case bStatus.statusBS of
      "completed" -> pure . Right $ BatchCompleted
      "failed" -> pure . Right $ BatchFailed (fromMaybe "" bStatus.errorFileIdBS)
      "cancelled" -> pure . Right $ BatchCancelled (fromMaybe "" bStatus.errorFileIdBS)
      "finalizing" -> pure . Right $ BatchFinalizing
      "in_progress" -> pure . Right $ BatchInProgress
      "validating" -> pure . Right $ BatchValidating
      _ -> pure . Left $ "@[getBatchStatus] unknown batch status: " <> T.unpack bStatus.statusBS


getFileContent :: Manager -> String -> String -> IO LBS.ByteString
getFileContent manager apiKey fid = do
  req0 <- parseRequest ("https://api.openai.com/v1/files/" ++ fid ++ "/content")
  let req = req0 { method = "GET", requestHeaders = authHeaders apiKey }
  resp <- httpLbs req manager
  pure resp.responseBody


parseBatchStatus :: A.Value -> At.Parser BatchStatus
parseBatchStatus = A.withObject "Batch" $ \o -> do
  idN <- o A..:  "id"
  statusN <- o A..:  "status"
  ouptputFileIdN <- o A..:? "output_file_id"
  errorFileIdN  <- o A..:? "error_file_id"
  requestCountsN <- o A..:? "request_counts"
  pure $ BatchStatus idN statusN ouptputFileIdN errorFileIdN requestCountsN


innerGetBatchStatus :: Manager -> String -> (UUID, String) -> IO (Either (String, LBS.ByteString) BatchStatus)
innerGetBatchStatus manager apiKey (bid, providerBatchId) = do
  req0 <- parseRequest ("https://api.openai.com/v1/batches/" <> providerBatchId)
  let req = req0 { method = "GET", requestHeaders = authHeaders apiKey }
  resp <- httpLbs req manager
  case A.eitherDecode (responseBody resp) >>= At.parseEither parseBatchStatus of
    Left errMsg -> pure . Left $ (errMsg, resp.responseBody)
    Right bStatus -> pure $ Right bStatus


fetchBatchResult :: Manager -> String -> (UUID, String) -> IO (Either String (LBS.ByteString, V.Vector (T.Text, T.Text)))
fetchBatchResult manager apiKey (bid, providerBatchId) = do
  innerGetBatchStatus manager apiKey (bid, providerBatchId) >>= \case
    Left (errMsg, respBody) -> pure . Left $ "@[fetchBatchResult] innerGetBatchStatus err: " <> errMsg <> ", response: " <> show respBody
    Right bStatus -> case bStatus.outputFileIdBS of
      Nothing -> pure . Left $ "@[fetchBatchResult] missing output_file_id for batch: " <> providerBatchId
      Just ofid -> do
        jsonl <- getFileContent manager apiKey (T.unpack ofid)
        let
          linesBS = filter (not . LBS.null) (LBS.split 0x0A jsonl) -- split on '\n'
          (downloaded, skipped) = foldr (\line (rezAccum, errAccum) ->
                  case A.eitherDecode' line :: Either String A.Value of    -- First, use the direct extraction; then move to Result decoding.
                    Left erMsg -> (rezAccum, erMsg : errAccum)
                    Right aVal -> case extractMainText aVal of
                      Left errMsg -> (rezAccum, errMsg : errAccum)
                      Right content -> (content : rezAccum, errAccum)
                ) ([], []) linesBS
        pure $ Right (jsonl, V.fromList downloaded)


extractMainText :: A.Value -> Either String (T.Text, T.Text)
extractMainText v =
  case At.parseEither parseFromRoot v of
    Right (requestID, content) | not (T.null (T.strip content)) -> Right (requestID, T.strip content)
    Left erMsg -> Left erMsg


parseFromRoot :: A.Value -> At.Parser (T.Text, T.Text)
parseFromRoot = A.withObject "root" $ \o -> do
  customId <- o .: "custom_id" :: At.Parser T.Text
  mResp <- o .:? "response"
  content <- case mResp of
    Just (A.Object ro) -> do
      body <- ro .: "body"
      parseFromBody body
    _ ->
      -- Fallback: try to parse this object directly as a Responses body (for non-batch use)
      parseFromBody (A.Object o)
  pure (customId, content)

parseFromBody :: A.Value -> At.Parser T.Text
parseFromBody = A.withObject "body" $ \bo -> do
  -- 1) Try convenience field `output_text` (string or array of strings)
  mOT  <- bo .:? "output_text" :: At.Parser (Maybe A.Value)
  let
    fromOT = case mOT of
        Just (A.String t) -> [t]
        Just (A.Array arr)-> [ t | A.String t <- V.toList arr ]
        _                 -> []

  -- 2) Canonical `output` array ⇒ collect message.content[].text
  mOut <- bo .:? "output"
  fromOutput <- case mOut of
    Just (A.Array arr) -> pure (concatMap collectFromOutputItem (V.toList arr))
    _                  -> pure []

  let
    texts = filter (not . T.null) (map T.strip (fromOT ++ fromOutput))
  pure $ if null texts then T.empty else T.intercalate "\n\n" texts

collectFromOutputItem :: A.Value -> [T.Text]
collectFromOutputItem v =
  case At.parseEither (A.withObject "out_item" $ \oi -> do
         ty <- oi .:? "type" :: At.Parser (Maybe T.Text)
         case ty of
           Just "message" -> do
             mc <- oi .:? "content"
             case mc of
               Just (A.Array carr) -> pure (concatMap collectFromContent (V.toList carr))
               _                   -> pure []
           _ -> pure []
       ) v of
    Right xs -> xs
    Left  _  -> []

collectFromContent :: A.Value -> [T.Text]
collectFromContent v =
  case At.parseEither (A.withObject "content" $ \co -> do
         -- keep it flexible: if a `text` field exists, use it (covers type=output_text|text)
         mt <- co .:? "text" :: At.Parser (Maybe T.Text)
         pure (maybeToList mt)
       ) v of
    Right xs -> xs
    Left  _  -> []


-- Full extraction:

{-
The JSON for the result of a prompt processing is:
{
  "id": "batch_req_6974c57323548190a42642f185a6f7ec"
, "custom_id": "q_260124_131142_0001"
, "response": {
    "status_code": 200
  , "request_id": "4ed39d57-cde3-4e59-96c0-dea1135de318"
  , "body": {
      "id": "resp_03777219f1a0df8e006974c555b8748193a9390cc985bda903"
    , "object": "response"
    , "created_at": 1769260373
    , "status": "completed"
    , "background": false
    , "billing": {"payer": "developer"}
    , "completed_at": 1769260375
    , "error": null
    , "frequency_penalty": 0.0
    , "incomplete_details": null
    , "instructions": null
    , "max_output_tokens": 200000
    , "max_tool_calls": null
    , "model": "gpt-5.2-2025-12-11"
    , "output": [
        {"id": "rs_03777219f1a0df8e006974c555f2e881939e28907af284e3e2", "type": "reasoning", "summary": []}
      , {
          "id": "msg_03777219f1a0df8e006974c55765b48193ba8093169f68ee7c", "type": "message", "status": "completed"
          , "content": [
              {
                "type": "output_text"
              , "annotations": []
              , "logprobs": []
              , "text": "\u201cThe content.\u201d has:\n\n- **2 words**\n- **12 characters** (including the space and the period)"
              }
            ]
          , "role": "assistant"
        }
      ]
    , "parallel_tool_calls": true
    , "presence_penalty": 0.0
    , "previous_response_id": null
    , "prompt_cache_key": "doc-fnv1a64-8332a56f103a1dd3"
    , "prompt_cache_retention": null
    , "reasoning": {"effort": "high", "summary": null}
    , "safety_identifier": null
    , "service_tier": "default"
    , "store": true
    , "temperature": 1.0
    , "text": {"format": {"type": "text"}, "verbosity": "medium"}
    , "tool_choice": "auto"
    , "tools": []
    , "top_logprobs": 0
    , "top_p": 0.98
    , "truncation": "disabled"
    , "usage": {
          "input_tokens": 30
        , "input_tokens_details": {"cached_tokens": 0}
        , "output_tokens": 184
        , "output_tokens_details": {"reasoning_tokens": 155}
        , "total_tokens": 214
      }
    , "user": null
    , "metadata": {}
    }
  }
, "error": null
}

-}

data Result = Result {
  idR :: Text
  , customIdR :: Text
  , responseR :: ProcessingResponse
  , errorR :: Maybe Text
} deriving (Show, Generic)

instance A.FromJSON Result where
  parseJSON = A.withObject "Result" $ \o ->
    Result <$> o A..: "id"
      <*> o A..: "custom_id"
      <*> o A..: "response"
      <*> o A..:? "error"

data ProcessingResponse = ProcessingResponse {
  statusCodeR :: Int
  , requestIdR :: Text
  , bodyR :: Body
} deriving (Show, Generic)

instance A.FromJSON ProcessingResponse where
  parseJSON = A.withObject "Response" $ \o ->
    ProcessingResponse <$> o A..: "status_code"
      <*> o A..: "request_id"
      <*> o A..: "body"

data Body = Body {
  idB :: Text
  , objectB :: Text
  , createdAtB :: Int
  , statusB :: Text
  , backgroundB :: Bool
  , billingB :: Billing
  , completedAtB :: Int
  , errorB :: Maybe Text
  , frequencyPenaltyB :: Double
  , incompleteDetailsB :: Maybe Text
  , instructionsB :: Maybe Text
  , maxOutputTokensB :: Int
  , maxToolCallsB :: Maybe Int
  , modelB :: Text
  , outputB :: [Output]
  , parallelToolCallsB :: Bool
  , presencePenaltyB :: Double
  , previousResponseIdB :: Maybe Text
  , promptCacheKeyB :: Text
  , promptCacheRetentionB :: Maybe Text
  , reasoningB :: Reasoning
  , safetyIdentifierB :: Maybe Text
  , serviceTierB :: Text
  , storeB :: Bool
  , temperatureB :: Double
  , textB :: Text
  , toolChoiceB :: Text
  , toolsB :: [Tool]
  , topLogprobsB :: Int
  , topPB :: Double
  , truncationB :: Text
  , usageB :: Usage
  , userB :: Maybe Text
  , metadataB :: Maybe Text
} deriving (Show, Generic)

instance A.FromJSON Body where
  parseJSON = A.withObject "Body" $ \o ->
    Body <$> o A..: "id"
      <*> o A..: "object"
      <*> o A..: "created_at"
      <*> o A..: "status"
      <*> o A..: "background"
      <*> o A..: "billing"
      <*> o A..: "completed_at"
      <*> o A..: "error"
      <*> o A..: "frequency_penalty"
      <*> o A..: "incomplete_details"
      <*> o A..: "instructions"
      <*> o A..: "max_output_tokens"
      <*> o A..: "max_tool_calls"
      <*> o A..: "model"
      <*> o A..: "output"
      <*> o A..: "parallel_tool_calls"
      <*> o A..: "presence_penalty"
      <*> o A..: "previous_response_id"
      <*> o A..: "prompt_cache_key"
      <*> o A..: "prompt_cache_retention"
      <*> o A..: "reasoning"
      <*> o A..: "safety_identifier"
      <*> o A..: "service_tier"
      <*> o A..: "store"
      <*> o A..: "temperature"
      <*> o A..: "text"
      <*> o A..: "tool_choice"
      <*> o A..: "tools"
      <*> o A..: "top_logprobs"
      <*> o A..: "top_p"
      <*> o A..: "truncation"
      <*> o A..: "usage"
      <*> o A..: "user"
      <*> o A..: "metadata"

newtype Billing = Billing {
  payerB :: Text
} deriving (Show, Generic)

instance A.FromJSON Billing where
  parseJSON = A.withObject "Billing" $ \o ->
    Billing <$> o A..: "payer"


data Reasoning = Reasoning {
  effortB :: Text
  , summaryB :: Maybe Text
} deriving (Show, Generic)

instance A.FromJSON Reasoning where
  parseJSON = A.withObject "Reasoning" $ \o ->
    Reasoning <$> o A..: "effort"
      <*> o A..:? "summary"

data Output = Output {
  idO :: Text
  , typeO :: Text
  , statusO :: Text
  , contentO :: [Content]
} deriving (Show, Generic)
instance A.FromJSON Output where
  parseJSON = A.withObject "Output" $ \o ->
    Output <$> o A..: "id"
      <*> o A..: "type"
      <*> o A..: "status"
      <*> o A..: "content"

data Content = Content {
  typeC :: Text
  , annotationsC :: [Annotation]
  , logprobsC :: [Logprob]
  , textC :: Text
} deriving (Show, Generic)

instance A.FromJSON Content where
  parseJSON = A.withObject "Content" $ \o ->
    Content <$> o A..: "type"
      <*> o A..: "annotations"
      <*> o A..: "logprobs"
      <*> o A..: "text"

data Annotation = Annotation {
  typeA :: Text
  , textA :: Text
} deriving (Show, Generic)

instance A.FromJSON Annotation where
  parseJSON = A.withObject "Annotation" $ \o ->
    Annotation <$> o A..: "type"
      <*> o A..: "text"

data Logprob = Logprob {
  indexL :: Int
  , logprobL :: Double
} deriving (Show, Generic)

instance A.FromJSON Logprob where
  parseJSON = A.withObject "Logprob" $ \o ->
    Logprob <$> o A..: "index"
      <*> o A..: "logprob"

data Tool = Tool {
  idT :: Text
  , typeT :: Text
  , statusT :: Text
  , contentT :: [Content]
} deriving (Show, Generic)
instance A.FromJSON Tool where
  parseJSON = A.withObject "Tool" $ \o ->
    Tool <$> o A..: "id"
      <*> o A..: "type"
      <*> o A..: "status"
      <*> o A..: "content"


data Usage = Usage {
  inputTokensU :: Int
  , inputTokensDetailsU :: Maybe InputTokensDetailsU
  , outputTokensU :: Int
  , outputTokensDetailsU :: Maybe OutputTokensDetailsU
  , totalTokensU :: Int
} deriving (Show, Generic)
instance A.FromJSON Usage where
  parseJSON = A.withObject "Usage" $ \o ->
    Usage <$> o A..: "input_tokens"
      <*> o A..:? "input_tokens_details"
      <*> o A..: "output_tokens"
      <*> o A..:? "output_tokens_details"
      <*> o A..: "total_tokens"

newtype InputTokensDetailsU = InputTokensDetailsU {
  cachedTokensU :: Int
} deriving (Show, Generic)
instance A.FromJSON InputTokensDetailsU where
  parseJSON = A.withObject "InputTokensDetailsU" $ \o ->
    InputTokensDetailsU <$> o A..: "cached_tokens"

newtype OutputTokensDetailsU = OutputTokensDetailsU {
  reasoningTokensU :: Int
} deriving (Show, Generic)
instance A.FromJSON OutputTokensDetailsU where
  parseJSON = A.withObject "OutputTokensDetailsU" $ \o ->
    OutputTokensDetailsU <$> o A..: "reasoning_tokens"

