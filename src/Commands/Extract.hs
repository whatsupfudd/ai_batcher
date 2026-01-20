-- "extract" utility — first draft
-- Reads a JSONL file produced by the batch results, extracts the main text
-- content from each line, concatenates it, writes <prefix>.txt (raw text), and
-- <prefix>.html where the text is first converted from Markdown to HTML and
-- injected into a simple template.
--
-- Usage:
--   stack run extract -- results.jsonl out/extracted
-- Produces:
--   out/extracted.txt
--   out/extracted.html

module Commands.Extract (doExtract) where

import Control.Monad (foldM)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

import qualified Data.Aeson as A
import qualified Data.Aeson.Types as At
import Data.Aeson ( (.:), (.:?) )

import CMark (commonmarkToHtml, optSmart, optSafe)
import Data.Maybe (maybeToList)

import qualified Options.Runtime as Opt


doExtract :: FilePath -> FilePath -> Opt.RunOptions -> IO ()
doExtract jsonlPath outPrefix rtOpts = do
  lbs <- LBS.readFile jsonlPath
  let
    linesBS = filter (not . LBS.null) (LBS.split 0x0A lbs) -- split on '\n'
  (texts, total, ok, bad) <- foldM step ([], 0::Int, 0::Int, 0::Int) linesBS
  -- putStrLn $ "@[doExtract] texts: " <> show texts
  let
    joined = T.intercalate "\n\n---\n\n" (reverse texts)

  -- Write raw text
  let
    htmlPath = outPrefix ++ ".html"
    txtPath  = outPrefix ++ ".txt"
  -- TIO.writeFile txtPath joined

  -- Markdown -> HTML and inject into template
  let
    title = T.breakOn "\n" joined
    htmlFrag = commonmarkToHtml [optSmart, optSafe] joined
    fullHtml = renderTemplate (fst title) htmlFrag
  BS.writeFile htmlPath (TE.encodeUtf8 fullHtml)

  putStrLn $ "Processed lines: " ++ show total
  putStrLn $ "Extracted items: " ++ show ok ++ ", skipped: " ++ show bad
  putStrLn $ "Wrote: " ++ txtPath ++ ", " ++ htmlPath

  where
    step (acc, total, ok, bad) bs =
      case A.eitherDecode' bs :: Either String A.Value of
        Left _ -> pure (acc, total+1, ok, bad+1)
        Right v -> case extractMainText v of
          Just t  -> pure (t:acc, total+1, ok+1, bad)
          Nothing -> pure (acc, total+1, ok, bad+1)

{-- Try to robustly extract the main textual content from a Responses API result line.
-- We support multiple shapes:
--  * current output from OpenAI:
  { "id": "batch_req_68f61e6bc00c8190b48201a6f13e22fe"
    , "custom_id": "q-0003"
    , "response": {
        "status_code": 200
      , "request_id": "618aecd15c516a20245ec7138805580d"
      , "body": {
          "id": "resp_00c0e3df9b965e720068f61d1fa0948197a75d637606e7ea24"
        , "object": "response"
        , "created_at": 1760959775
        , "status": "completed"
        , "background": false
        , "billing": {"payer": "developer"}
        , "error": null
        , "incomplete_details": null
        , "instructions": null
        , "max_output_tokens": 200000
        , "max_tool_calls": null
        , "model": "gpt-5-2025-08-07"
        , "output": [
              {
                  "id": "rs_00c0e3df9b965e720068f61d200c60819788a7a3ca5f9bffff"
                , "type": "reasoning"
                , "summary": []
              }
            , {
                  "id": "msg_00c0e3df9b965e720068f61d3b711881979c04d29769848faa"
                , "type": "message"
                , "status": "completed"
                , "content": [
                    {
                      "type": "output_text"
                      , "annotations": []
                      , "logprobs": []
                      , "text": "<content>"
                    }
                ]
              }
          ]
        }
--   • { "output_text": "..." }
--   • { "output": [ { "content": [ {"type":"output_text"|"text", "text":"..."}, ... ] }, ... ] }
--   • { "message": { "content": [ {"type":"text"|"output_text", "text":"..."}, ... ] } }
--   • legacy-ish: { "choices": [ { "message": { "content": [ {"type":"text","text":"..."} ] } } ] }
--}
-- Drop-in replacement for the extractor's text pull logic.
-- Fixes shape: batch results wrap Responses objects under response.body.*
--


extractMainText :: A.Value -> Maybe T.Text
extractMainText v =
  case At.parseEither parseFromRoot v of
    Right t | not (T.null (T.strip t)) -> Just (T.strip t)
    _ -> Nothing

-- Batch JSON lines look like:
-- {
--   "id": "batch_req_*",
--   "custom_id": "q-0003",
--   "response": {
--     "status_code": 200,
--     "body": {
--        "id": "resp_*",
--        "output_text": ["..."],              -- optional convenience
--        "output": [                            -- canonical
--           { "type":"reasoning", ... },
--           { "type":"message", "content":[ {"type":"output_text","text":"..."} ] }
--        ]
--     }
--   }
-- }

parseFromRoot :: A.Value -> At.Parser T.Text
parseFromRoot = A.withObject "root" $ \o -> do
  mResp <- o .:? "response"
  case mResp of
    Just (A.Object ro) -> do
      body <- ro .: "body"
      parseFromBody body
    _ ->
      -- Fallback: try to parse this object directly as a Responses body (for non-batch use)
      parseFromBody (A.Object o)

parseFromBody :: A.Value -> At.Parser T.Text
parseFromBody = A.withObject "body" $ \bo -> do
  -- 1) Try convenience field `output_text` (string or array of strings)
  mOT  <- bo .:? "output_text" :: At.Parser (Maybe A.Value)
  let fromOT = case mOT of
        Just (A.String t) -> [t]
        Just (A.Array arr)-> [ t | A.String t <- V.toList arr ]
        _                 -> []

  -- 2) Canonical `output` array ⇒ collect message.content[].text
  mOut <- bo .:? "output"
  fromOutput <- case mOut of
    Just (A.Array arr) -> pure (concatMap collectFromOutputItem (V.toList arr))
    _                  -> pure []

  let texts = filter (not . T.null) (map T.strip (fromOT ++ fromOutput))
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

-- Simple HTML template with the converted Markdown injected into a child <div>
renderTemplate :: T.Text -> T.Text -> T.Text
renderTemplate title htmlFrag = T.concat
  [ "<!doctype html>\n<html lang=\"en\">\n<head>\n"
  , "<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
  , "<title>" <> title <> "</title>\n"
  , "<style>body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.6;padding:24px}"
  , ".container{max-width:900px;margin:0 auto}.prose h1,h2,h3{line-height:1.25}</style>\n"
  , "</head>\n<body>\n<main class=\"container\">\n<div id=\"content\" class=\"prose\">\n"
  , htmlFrag
  , "\n</div>\n</main>\n</body>\n</html>\n"
  ]
