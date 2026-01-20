{-# LANGUAGE DeriveGeneric #-}
-- OpenAI Batch Cache Loader — v0.2
-- Change: do NOT use Files API for the reference doc anymore.
-- Instead, read a plain-text document, PREPEND its content to each prompt, and
-- set a shared `prompt_cache_key` so OpenAI's prompt caching can reuse the long
-- preamble across all requests in the batch.
--
-- CLI usage:
--   OPENAI_API_KEY=sk-... [OPENAI_MODEL=gpt-5] [OPENAI_REASONING_EFFORT=high]
--   [OPENAI_PROMPT_CACHE_KEY=your-id]
--   stack run -- doc.txt prompts.txt out.json
--
-- Notes:
--   • If OPENAI_PROMPT_CACHE_KEY is not provided, we derive a stable key from
--     the document using a built-in FNV-1a 64-bit hash: "doc-fnv1a64-<hex>".
--   • We still use the Batch API flow: create JSONL (one /v1/responses request
--     per prompt), upload JSONL as purpose "batch", POST /v1/batches.
--   • Extended reasoning supported via reasoning.effort = "high" (default) or
--     set OPENAI_REASONING_EFFORT=medium.

module Commands.Load (doLoad) where

import Control.Monad ( when )
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Data.Bits ( xor )
import Data.Maybe ( fromMaybe )
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Encoding as TE
import Data.Word ( Word64, Word8 )
import Numeric ( showHex )
import qualified Data.Text.Read as TR
import Data.Char (isSpace)
import Data.Time.Clock ( getCurrentTime )
import qualified Data.Time.Format as Dfm

import System.Environment ( getArgs, lookupEnv )
import System.Exit ( exitFailure )
import System.IO ( hPutStrLn, stderr )

import Data.Aeson ( (.=) )
import qualified Data.Aeson                   as A
import qualified Data.Aeson.Encode.Pretty     as AP
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

import GHC.Generics ( Generic )

import qualified Options.Runtime as Opt


-- -------------------------------
-- Types
-- -------------------------------

data PersistInfo = PersistInfo
  { prompt_cache_key    :: T.Text
  , document_hash_fnv64 :: T.Text
  , batch_input_file_id :: T.Text
  , batch_id            :: T.Text
  , endpoint            :: T.Text
  , completion_window   :: T.Text
  , model_used          :: T.Text
  } deriving (Show, Generic)

instance A.ToJSON PersistInfo

-- -------------------------------
-- Main
-- -------------------------------

doLoad :: FilePath -> FilePath -> FilePath -> Opt.RunOptions -> IO ()
doLoad docPath promptsPath outPath rtOpts = do
  mKey <- lookupEnv "OPENAI_API_KEY"
  key  <- maybe (die "Missing OPENAI_API_KEY in env") pure mKey
  mModel <- lookupEnv "OPENAI_MODEL"
  let model = T.pack $ fromMaybe "gpt-5" mModel
  mEffort <- lookupEnv "OPENAI_REASONING_EFFORT" -- "high"|"medium"
  let reasoningEffort = T.pack $ fromMaybe "high" mEffort

  -- 1) Read the reference document (plain text)
  docText <- TIO.readFile docPath
  let docBS      = TE.encodeUtf8 docText
      docHashW64 = fnv1a64 docBS
      docHashHex = T.pack (toHex64 docHashW64)

  -- 2) Determine prompt_cache_key
  mPCK <- lookupEnv "OPENAI_PROMPT_CACHE_KEY"
  let cacheKey = T.pack $ fromMaybe ("doc-fnv1a64-" ++ T.unpack docHashHex) mPCK

  -- 3) Read prompts file (block format: "N: Title" separated by blank lines)
  rawPrompts <- TIO.readFile promptsPath
  let rewrittenPrompts = parseAndRewritePrompts rawPrompts
  when (null rewrittenPrompts) $
    die ("No valid prompts found in file: " <> promptsPath <> ". Expected blocks like: <number>: <title> separated by blank lines.")

  -- 4) Build JSONL for Batch API (prepend docText + share prompt_cache_key)
  moment <- getCurrentTime
  let
    fmtMoment = Dfm.formatTime Dfm.defaultTimeLocale "%y%m%d_%H%M%S" moment
  let jsonlText = T.intercalate "\n"
        [ oneLineJSONL i fmtMoment p docText cacheKey model reasoningEffort | (i, p) <- zip [(1::Int)..] rewrittenPrompts ]
      jsonlBytes = TE.encodeUtf8 jsonlText
      jsonlPath  = "batch_input_" <> fmtMoment <> ".jsonl"
  LBS.writeFile jsonlPath (LBS.fromStrict jsonlBytes)
  putStrLn $ "Created JSONL with " ++ show (length rewrittenPrompts) ++ " requests: " ++ jsonlPath

  -- 5) Upload JSONL (purpose: "batch")
  manager <- newManager tlsManagerSettings
  putStrLn "Uploading batch input file..."
  inputFileId <- uploadFile manager key jsonlPath "batch"
  putStrLn $ "Batch input_file_id: " ++ T.unpack inputFileId

  -- 6) Create batch
  putStrLn "Creating batch..."
  let endpointPath = "/v1/responses" :: T.Text
      window       = "24h"            :: T.Text
  batchId <- createBatch manager key inputFileId endpointPath window
  putStrLn $ "Batch created. id=" ++ T.unpack batchId

  -- 7) Persist identifiers for later retrieval
  let out = PersistInfo
              { prompt_cache_key    = cacheKey
              , document_hash_fnv64 = docHashHex
              , batch_input_file_id = inputFileId
              , batch_id            = batchId
              , endpoint            = endpointPath
              , completion_window   = window
              , model_used          = model
              }
  LBS.writeFile outPath (AP.encodePretty out)
  putStrLn $ "Saved identifiers to " ++ outPath



-- Replace the prompts-reading section inside runOnce with the following:

-- Helpers for parsing and rewriting the prompt file --------------------------

parseAndRewritePrompts :: T.Text -> [T.Text]
parseAndRewritePrompts txt =
  [ rewritePrompt n title
  | blk <- splitBlocks txt
  , Just (n, title) <- [parseBlock blk]
  ]

-- Split on one-or-more blank lines (robust to CRLF and stray spaces)
splitBlocks :: T.Text -> [T.Text]
splitBlocks t = go [] [] (T.lines (normalizeNewlines t))
  where
    normalizeNewlines = T.replace "\r\n" "\n"
    flush acc cur = if null cur then acc else T.strip (T.unlines (reverse cur)) : acc
    go acc cur [] = reverse (flush acc cur)
    go acc cur (l:ls)
      | T.all isSpace l = go (flush acc cur) [] ls
      | otherwise       = go acc (l:cur) ls

-- Parse a block of the form: "<number> : <title>" (title may include braces, dashes, unicode, etc.)
parseBlock :: T.Text -> Maybe (Int, T.Text)
parseBlock blk =
  case T.breakOn ":" (T.strip blk) of
    (numTxt, rest) | not (T.null rest) ->
      case TR.decimal (T.strip numTxt) of
        Right (n, remTxt) | T.all isSpace remTxt ->
          let title = T.strip (T.drop 1 rest) -- drop ':'
          in if T.null title then Nothing else Just (n, title)
        _ -> Nothing
    _ -> Nothing

-- Render the new prompt format
rewritePrompt :: Int -> T.Text -> T.Text
rewritePrompt n title = T.concat
  [ "Generate a first version for item ", T.pack (show n), ", \"", title, "\"" ]


-- -------------------------------
-- Helpers
-- -------------------------------

die :: String -> IO a
die msg = hPutStrLn stderr ("[FATAL] " ++ msg) >> exitFailure

trim :: String -> String
trim = f . f where f = reverse . dropWhile (`elem` ['\n','\r','\t',' '])

prefixText :: T.Text
prefixText = "As an expert in legal matters for financial instrument structuring in Europe, especially in Securitisation Vehicle law in Luxembourg, you are tasked by management in doing detailed drafting in a project that aims at establishing a set of entities that will implement a tokenized security concept based on bundles that create diversification and where notes have a 1-to-1 linkage to their underlying assets (most being physical assets). You draft your documents using the form and language typical of what a high-quality law firm established in Luxembourg will use, apply consistent numbering using digits/numbers rather than letters to create a clear and easily referentiable hierarchy. We provide you with a description of the project, a reference version of the Scope of Work (SoW), some detailed deliverables information, and the list of the documents that make up the entire set of deliverables to draft:\n"

postfixText :: T.Text
postfixText = "Use English as the working language, start each draft with the title of the deliverable, then add a short technical block with the item ID of the draft, the reference to the detailed deliverable description in the subtitle/initial comments of the document and any additional referencing info that is part of the detailed description for this item, use markdown format to create font variations (bold, italic, etc), provide definitions in the drafts and add some closing notes to counsel or client when relevant."

-- Compose one line of Batch JSONL for /v1/responses where we prepend the document
-- to the conversation and use a shared prompt_cache_key
oneLineJSONL :: Int -> String -> T.Text -> T.Text -> T.Text -> T.Text -> T.Text -> T.Text
oneLineJSONL i moment prompt docText cacheKey model effort =
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
                                           , "text" .= T.concat [
                                                prefixText, docText
                                                , "\n\n\nUse these project details as the authoritative context for your responses.\n"
                                                , postfixText
                                               ]
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

-- Upload a file to the Files API with a specific purpose ("batch").
uploadFile :: Manager -> String -> FilePath -> T.Text -> IO T.Text
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
    Left e  -> die ("Files API decode error: " ++ e)
    Right v -> case v of
      A.Object o -> case At.parseMaybe (A..: "id") o of
        Just (A.String fid) -> pure fid
        _ -> do
          putStrLn "Response: " >> print v
          die "Files API: no 'id' in response"
      _ -> die "Files API: unexpected response"

-- Create a batch from an uploaded JSONL file
createBatch :: Manager -> String -> T.Text -> T.Text -> T.Text -> IO T.Text
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
    Left e  -> die ("Batch create decode error: " ++ e)
    Right v -> case v of
      A.Object o -> case At.parseMaybe (A..: "id") o of
        Just (A.String bid) -> pure bid
        _ -> do
          putStrLn "Response: " >> print v
          die "Batch create: no 'id' in response"
      _ -> die "Batch create: unexpected response"

-- Common auth headers
authHeaders :: String -> [(HeaderName, BS8.ByteString)]
authHeaders key =
  [ ("Authorization", BS8.pack ("Bearer " <> key))
  , ("OpenAI-Beta", "prompt-caching-1") -- harmless if ignored
  ]
