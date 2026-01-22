module Service.OpenAI where

import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import GHC.Generics ( Generic )

import qualified Data.Aeson as A
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
import Network.HTTP.Client.MultipartFormData ( partBS, partFile, formDataBody )
import Network.HTTP.Types (HeaderName)


oneLineJSONL :: Int -> String -> T.Text -> T.Text -> T.Text -> T.Text -> T.Text -> T.Text
oneLineJSONL i moment prompt contentText cacheKey model effort =
  let customId  = T.pack ("q_" <> moment <> "_" <> pad4 i)
      body :: A.Value
      body = A.object
        [ "model"            .= model
        , "prompt_cache_key" .= cacheKey
        , "reasoning"        .= A.object ["effort" .= effort]
        , "max_output_tokens" .= A.Number 200000
        , "input"            .= A.toJSON
            [ A.object
                [ "role"    .= ("system" :: T.Text)
                , "content" .= [ A.object ["type" .= ("input_text" :: T.Text)
                                           , "text" .= contentText
                                           ]
                               ]
                ]
            , A.object
                [ "role"    .= ("user" :: T.Text)
                , "content" .= [ A.object ["type"   .= ("input_text" :: T.Text)
                                           , "text"   .= prompt
                                           ]
                               ]
                ]
            ]
        ]
      lineObj = A.object
        [ "custom_id" .= customId
        , "method"    .= ("POST" :: T.Text)
        , "url"       .= ("/v1/responses" :: T.Text)
        , "body"      .= body
        ]
  in TE.decodeUtf8 (LBS.toStrict (A.encode lineObj))

uploadFile :: Manager -> String -> FilePath -> T.Text -> IO (Either String T.Text)
uploadFile manager apiKey filePath purpose = do
  baseReq <- parseRequest "https://api.openai.com/v1/files"
  req <- formDataBody
            [ partFile "file" filePath
            , partBS   "purpose" (TE.encodeUtf8 purpose)
            -- , partBS   "filename" (TE.encodeUtf8 (T.pack name))
            ]
            baseReq { method = "POST"
                    , requestHeaders = authHeaders apiKey
                    }
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
createBatch :: Manager -> String -> T.Text -> T.Text -> T.Text -> IO (Either String T.Text)
createBatch manager apiKey inputFileId endpointPath completionWindow = do
  initReq <- parseRequest "https://api.openai.com/v1/batches"
  let payload = A.object
        [ "input_file_id"     .= inputFileId
        , "endpoint"          .= endpointPath
        , "completion_window" .= completionWindow
        ]
      req = initReq { method = "POST"
                    , requestHeaders = ("Content-Type", "application/json") : authHeaders apiKey
                    , requestBody    = RequestBodyLBS (A.encode payload)
                    }
  resp <- httpLbs req manager
  case A.eitherDecode (responseBody resp) of
    Left e  -> pure . Left $ "@[createBatch] decode error: " <> e
    Right v -> case v of
      A.Object o -> case At.parseMaybe (A..: "id") o of
        Just (A.String bid) -> pure $ Right bid
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

