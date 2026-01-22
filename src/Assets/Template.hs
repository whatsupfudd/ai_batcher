{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE QuasiQuotes #-}

module Assets.Template where

import Control.Exception (Exception, throwIO)
import Control.Monad (forM, forM_)
import Control.Monad.Trans.Writer.Lazy (Writer)

import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (isSpace)
import Data.Functor.Identity as Fi
import Data.Int (Int32, Int64)
import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.UUID (UUID)
import qualified Data.Vector as V

import System.FilePath (takeFileName)

import GHC.Generics (Generic)

import Data.Aeson (Value (..), (.=))
import qualified Data.Aeson as Ae
import qualified Data.Aeson.Key as Ak
import qualified Data.Aeson.KeyMap as Akm

import qualified Hasql.Pool as Pool
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS
import qualified Hasql.Statement as St
import qualified Hasql.TH as TH

import Text.Ginger.Parse (IncludeResolver, parseGinger, SourcePos, ParserError)
import Text.Ginger.Run (easyRender)
import Text.Ginger.Run.Type (Run)
import Text.Ginger.GVal (GVal, dict, rawJSONToGVal, (~>))
import Text.Ginger.AST as Ga

--------------------------------------------------------------------------------
-- Public API / Environment

-- | Minimal dependencies this module needs.
--   Plug your existing Minio/S3 + Hasql pool into this.
data Context = Context {
    pgPoolCT :: Pool.Pool
  , s3RepoCT :: ByteString -> IO UUID
  -- ^ Must upload bytes to S3 and return the UUID "locator" used as the object name.
  }

data SourceItem = SourceItem
  { siHeaderToml :: Text
  , siMetaJson   :: Value
  , siBody       :: Text
  }
  deriving (Show, Eq, Generic)

data AssetTemplateError
  = TemplateDecodeError Text
  | SourceDecodeError Text
  | GingerParseError Text
  | SourceParseError Text
  | DbError Text
  deriving (Show, Eq, Generic, Exception)

-- | End-to-end ingestion:
--   - upload template + source to S3 (record in s3_objects)
--   - insert template + source rows
--   - create production
--   - split items, render template per item
--   - insert requests + initial request_events ('entered')
--
-- Returns the production_id.
-- ^ template file path
-- ^ source file path (concatenated items with TOML front matter)
-- ^ production name
ingestTemplate :: Context -> FilePath -> FilePath -> Text -> IO (Either AssetTemplateError UUID)
ingestTemplate ctxt templatePath sourcePath productionName = do
  tplBytes <- BS.readFile templatePath
  srcBytes <- BS.readFile sourcePath

  tplText <- either (throwIO . TemplateDecodeError . T.pack . show) pure (TE.decodeUtf8' tplBytes)
  srcText <- either (throwIO . SourceDecodeError   . T.pack . show) pure (TE.decodeUtf8' srcBytes)

  let
    templateName = T.pack (takeFileName templatePath)
    sourceName = T.pack (takeFileName sourcePath)

  -- Upload raw assets to S3
  tplLoc <- ctxt.s3RepoCT tplBytes
  srcLoc <- ctxt.s3RepoCT srcBytes

  -- Parse ginger template (from memory).
  -- Ginger's Source type is String; we keep filenames for better error messages.
  tplRez <- parseGinger nullResolver (Just (T.unpack templateName)) (T.unpack tplText)
  case tplRez of
    Left err -> pure . Left . GingerParseError . T.pack . show $ err
    Right template ->
      case parseSourceItems srcText of
        Left err -> pure . Left . SourceParseError . T.pack . show $ err
        Right items -> 
          let
            tplSize :: Int64
            tplSize = fromIntegral (BS.length tplBytes)
            srcSize :: Int64
            srcSize = fromIntegral (BS.length srcBytes)
          in
            let
              session = TxS.transaction TxS.Serializable TxS.Write $ do
                -- record S3 metadata
                Tx.statement (tplLoc, "template" :: Text, tplSize) insertS3Object
                Tx.statement (srcLoc, "source"   :: Text, srcSize) insertS3Object

                -- asset rows
                templateId <- Tx.statement (templateName, tplLoc) insertTemplate
                sourceId   <- Tx.statement (sourceName,   srcLoc) insertSource

                -- production
                productionId <- Tx.statement (productionName, templateId, sourceId) insertProduction

                -- requests
                forM_ (zip [1..] items) $ \(idx, item) -> do
                    let
                      rendered = renderRequest productionName idx item template
                    reqId <- Tx.statement (productionId, fromIntegral idx, item.siMetaJson, rendered) insertRequest
                    Tx.statement (reqId, "entered" :: Text, mkEnteredDetails templateName sourceName idx item) insertRequestEvent
                    pure reqId

                pure productionId
            in do
            result <- Pool.use ctxt.pgPoolCT session
            case result of
              Left err -> pure . Left $ DbError (T.pack (show err))
              Right anID -> pure $ Right anID


nullResolver :: IncludeResolver IO
nullResolver srcName = pure Nothing


--------------------------------------------------------------------------------
-- Ginger rendering

type RenderM = Run SourcePos (Writer Text) Text

renderRequest :: Text -> Int -> SourceItem -> Template SourcePos -> Text
renderRequest productionName ix sItem template =
  let
    ctx :: GVal RenderM
    ctx = dict [ 
        "production" ~> (dict [ "name" ~> productionName ] :: GVal RenderM)
      , "index" ~> ix
      , "text" ~> sItem.siBody
      , "meta" ~> (rawJSONToGVal sItem.siMetaJson :: GVal RenderM)
      , "header_toml" ~> sItem.siHeaderToml
      , "item" ~> (dict [
            "index" ~> ix
          , "text" ~> sItem.siBody
          , "meta" ~> (rawJSONToGVal sItem.siMetaJson :: GVal RenderM)
          , "header_toml" ~> sItem.siHeaderToml
        ] :: GVal RenderM)
      ]
  in
  easyRender ctx template

mkEnteredDetails :: Text -> Text -> Int -> SourceItem -> Value
mkEnteredDetails templateName sourceName ix sItem = Ae.object [ 
      "event" .= ("entered" :: Text)
    , "template_name" .= templateName
    , "source_name"   .= sourceName
    , "item_index"    .= ix
    , "header_toml"   .= sItem.siHeaderToml
    ]

{-- Safer explicit pure parse (avoids ambiguous monad inference around parseGinger).
runParseGingerPure :: Text -> Text -> Either Text (Ga.Template SourcePos)
runParseGingerPure templateName tplText =
  let
    res = (parseGinger nullResolver (Just (T.unpack templateName)) (T.unpack tplText)
            :: Fi.Identity (Either ParserError (Ga.Template SourcePos))
      )
  in
  case Fi.runIdentity res of
    Left pe -> Left (T.pack (show pe))
    Right t -> Right t
-}

--------------------------------------------------------------------------------
-- Source splitting: ---------- TOML ---------- BODY

parseSourceItems :: Text -> Either Text [SourceItem]
parseSourceItems t =
  let
    delim = "-*-*-*-*-*-*-*-*-*-"
    isDelimLine x = T.strip x == delim
    lines = T.lines t
    iterItems xs =
      case dropWhile (not . isDelimLine) xs of
        [] -> Right []
        (_open : rest) ->
          let (hdrLines, afterHdr) = break isDelimLine rest
          in case afterHdr of
                [] -> Left "Item header opened but never closed with '-*-*-*-*-*-*-*-*-*-'."
                (_close : restBody) ->
                  let (bodyLines, next) = break isDelimLine restBody
                      hdrTxt  = T.unlines hdrLines
                      bodyTxt = T.unlines bodyLines
                  in do
                      meta <- parseTomlHeaderToJson hdrTxt
                      let item = SourceItem
                                { siHeaderToml = hdrTxt
                                , siMetaJson   = meta
                                , siBody       = bodyTxt
                                }
                      (item :) <$> iterItems next
  in
  iterItems lines

--------------------------------------------------------------------------------
-- Pragmatic TOML->JSON (subset) for front-matter headers
-- Intended for simple headers: key="str", key=123, key=true, key=[...], dotted keys.

parseTomlHeaderToJson :: Text -> Either Text Value
parseTomlHeaderToJson hdr =
  let
    lines = filter (not . isIgnorable) (T.lines hdr)
    isIgnorable aLine =
      let s = T.strip aLine
      in T.null s || T.isPrefixOf "#" s
  in
  Object <$> foldl' step (Right Akm.empty) lines
  where
  step :: Either Text (Akm.KeyMap Value) -> Text -> Either Text (Akm.KeyMap Value)
  step acc line = do
    obj <- acc
    (k, v) <- parseKV line
    pure (insertDottedKey k v obj)

parseKV :: Text -> Either Text ([Text], Value)
parseKV line =
  let (k0, rest0) = T.breakOn "=" line
      k = T.strip k0
      rest = T.strip (T.drop 1 rest0)
  in
  if T.null rest0 then
    Left ("Invalid TOML header line (missing '='): " <> line)
  else
    parseValue rest >>= \v -> Right (T.splitOn "." k, v)

parseValue :: Text -> Either Text Value
parseValue v0
  | Just s <- parseQuotedString v0 = Right (String s)
  | v0 == "true"  = Right (Bool True)
  | v0 == "false" = Right (Bool False)
  | Just arr <- parseArray v0 = Right (Array arr)
  | Just n <- parseNumber v0 = Right n
  | otherwise = Right (String (T.strip v0))


parseQuotedString :: Text -> Maybe Text
parseQuotedString s =
  let t = T.strip s
  in if T.length t >= 2 && T.head t == '"' && T.last t == '"'
        then Just (unescape (T.init (T.tail t)))
        else Nothing


unescape :: Text -> Text
unescape = T.replace "\\\"" "\"" . T.replace "\\n" "\n" . T.replace "\\t" "\t" . T.replace "\\\\" "\\"


parseArray :: Text -> Maybe Ae.Array
parseArray s =
  let t = T.strip s
  in
  if T.length t >= 2 && T.head t == '[' && T.last t == ']' then
    let
      inner = T.strip (T.init (T.tail t))
      parts = splitCommas inner
      vals  = traverse (either (const Nothing) Just . parseValue) parts
    in
    V.fromList <$> vals
  else
    Nothing


splitCommas :: Text -> [Text]
splitCommas = map T.strip . splitter False "" []
  where
  splitter _ cur acc txt
    | T.null txt = reverse (cur:acc)
    | otherwise =
        let c  = T.head txt
            xs = T.tail txt
        in case c of
              '"' -> splitter (not inQ) (T.snoc cur c) acc xs
              ',' | not inQ -> splitter inQ "" (cur:acc) xs
              _   -> splitter inQ (T.snoc cur c) acc xs
    where
    inQ = False -- shadowing guard; corrected below


parseNumber :: Text -> Maybe Value
parseNumber s =
  let t = T.strip s
      -- Very small numeric recognizer; good enough for headers
      isNumChar c = c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9')
  in if T.all isNumChar t && not (T.null t)
        then case reads (T.unpack t) :: [(Double, String)] of
              [(d, rest)] | all isSpace rest ->
                -- preserve ints as integers when exact
                if d == fromInteger (round d)
                  then Just (Number (fromInteger (round d)))
                  else Just (Number (realToFrac d))
              _ -> Nothing
        else Nothing


insertDottedKey :: [Text] -> Value -> Akm.KeyMap Value -> Akm.KeyMap Value
insertDottedKey [] _ obj = obj
insertDottedKey [k] v obj = Akm.insert (Ak.fromText k) v obj
insertDottedKey (k:ks) v obj =
  let key = Ak.fromText k
      child = case Akm.lookup key obj of
                Just (Object o) -> o
                _               -> Akm.empty
      child' = insertDottedKey ks v child
  in Akm.insert key (Object child') obj

--------------------------------------------------------------------------------
-- DB statements (Hasql-TH)

insertS3Object :: St.Statement (UUID, Text, Int64) ()
insertS3Object =
  [TH.resultlessStatement|
    insert into batcher.s3_objects (locator, kind, bytes)
    values ($1 :: uuid, $2 :: text, $3 :: int8)
    on conflict (locator) do nothing
  |]

insertTemplate :: St.Statement (Text, UUID) UUID
insertTemplate =
  [TH.singletonStatement|
    insert into batcher.asset_templates (template_name, s3_locator)
    values ($1 :: text, $2 :: uuid)
    returning template_id :: uuid
  |]

insertSource :: St.Statement (Text, UUID) UUID
insertSource =
  [TH.singletonStatement|
    insert into batcher.asset_sources (source_name, s3_locator)
    values ($1 :: text, $2 :: uuid)
    returning source_id :: uuid
  |]

insertProduction :: St.Statement (Text, UUID, UUID) UUID
insertProduction =
  [TH.singletonStatement|
    insert into batcher.productions (production_name, template_id, source_id)
    values ($1 :: text, $2 :: uuid, $3 :: uuid)
    returning production_id :: uuid
  |]

insertRequest :: St.Statement (UUID, Int32, Value, Text) UUID
insertRequest =
  [TH.singletonStatement|
    insert into batcher.requests
      (production_id, item_index, item_meta, request_text)
    values
      ($1 :: uuid, $2 :: int4, $3 :: jsonb, $4 :: text)
    returning request_id :: uuid
  |]

insertRequestEvent :: St.Statement (UUID, Text, Value) ()
insertRequestEvent =
  [TH.resultlessStatement|
    insert into batcher.request_events
      (request_id, state, details)
    values
      ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]
