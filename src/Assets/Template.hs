{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Assets.Template where

import Control.Exception (Exception, throwIO)
import Control.Monad (forM, forM_, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Writer.Lazy (Writer)

import qualified Data.ByteString.Lazy as Lbs
import Data.Char (isSpace)
import Data.Functor.Identity as Fi
import Data.Int (Int32, Int64)
import Data.List (foldl')
import Data.Maybe (isJust)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.UUID (UUID)
import Data.UUID as Uu
import qualified Data.UUID.V4 as Uu4
import qualified Data.Vector as V

import System.FilePath (takeFileName)

import GHC.Generics (Generic)

import Data.Aeson (Value (..), (.=))
import qualified Data.Aeson as Ae
import qualified Data.Aeson.Key as Ak
import qualified Data.Aeson.KeyMap as Akm

import Hasql.Pool (Pool, use)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS
import qualified Hasql.Session as Hs

import Text.Ginger.Parse (IncludeResolver, parseGinger, SourcePos, ParserError)
import Text.Ginger.Run (easyRender)
import Text.Ginger.Run.Type (Run)
import Text.Ginger.GVal (GVal, dict, rawJSONToGVal, (~>))
import Text.Ginger.AST as Ga

import qualified Options.Cli as Cli
import qualified DB.TemplateStmt as Ts
import qualified Assets.Types as At
import qualified Utils as Ut
import DB.EngineStmt (execStmt)


--------------------------------------------------------------------------------
-- Public API / Environment

-- | Minimal dependencies this module needs.
--   Plug your existing Minio/S3 + Hasql pool into this.
data Context = Context {
    pgPoolCT :: Pool
  , s3RepoCT :: Lbs.ByteString -> IO UUID
  , s3FetchCT :: Text -> IO (Either String Lbs.ByteString)
  -- ^ Must upload bytes to S3 and return the UUID "locator" used as the object name.
  }

data SourceItem = SourceItem {
    headerTomlSI :: Text
  , metaJsonSI   :: Value
  , bodySI       :: Text
  }
  deriving (Show, Eq, Generic)

data AssetTemplateError =
    TemplateDecodeError Text
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
ingestTemplate :: Context -> Cli.ProducerOpts -> IO (Either AssetTemplateError (UUID, Text))
ingestTemplate ctxt prodOpts =
  let
    mbTemplatePath = prodOpts.newTemplateSrcIG
    templateName = prodOpts.templateNameIG
    sourcePath = prodOpts.sourceIG
  in do
  productionName <- case prodOpts.productIG of
    Nothing -> T.pack . Uu.toString <$> Uu4.nextRandom
    Just productionName -> pure productionName

  putStrLn $ "@[ingestTemplate] production: " <> T.unpack productionName <> "."

  eiTmplText <- case mbTemplatePath of
    Nothing -> do
      eiMbEid <- use ctxt.pgPoolCT $ Hs.statement templateName Ts.fetchTemplate
      case eiMbEid of
        Left err -> pure . Left $ DbError (T.pack (show err))
        Right mbEid -> case mbEid of
          Nothing -> pure . Left $ TemplateDecodeError $ "@[ingestTemplate] template not found: " <> templateName
          Just (templateId, tplLoc) -> do
            eiTmplBytes <- ctxt.s3FetchCT (T.pack . Uu.toString $ tplLoc)
            case eiTmplBytes of
              Left err -> pure . Left $ DbError (T.pack ("@[ingestTemplate] s3FetchCT failed: " <> show err))
              Right tplBytes ->
                case TE.decodeUtf8' . Lbs.toStrict $ tplBytes of
                  Left err -> pure . Left $ TemplateDecodeError (T.pack ("@[ingestTemplate] decodeUtf8' failed: " <> show err))
                  Right tplText -> pure $ Right (templateId, tplLoc, tplText)
    Just templatePath -> do
      tplBytes <- Lbs.readFile templatePath
      case TE.decodeUtf8' . Lbs.toStrict $ tplBytes of
        Left err -> pure . Left $ TemplateDecodeError (T.pack ("@[ingestTemplate] decodeUtf8' failed: " <> show err))
        Right tplText ->
          let
            tplSize = fromIntegral $ T.length tplText
          in do
          tplLoc <- ctxt.s3RepoCT tplBytes
          eiTemplateId <- execStmt "ingestTemplate" ctxt.pgPoolCT $ do
            Tx.statement (tplLoc, "template" :: Text, tplSize) Ts.insertS3Object
            Tx.statement (templateName, tplLoc) Ts.insertTemplate
          case eiTemplateId of
            Left err -> pure . Left $ DbError (T.pack (show err))
            Right templateId -> pure $ Right (templateId, tplLoc, tplText)

  case eiTmplText of
    Left err -> pure . Left $ err
    Right (templateId, tplLoc, tplText) ->
        let
          sourceName = T.pack (takeFileName sourcePath)
          cacheKey = "doc-fnv1a64-" <> (T.pack . Ut.toHex64 . Ut.fnv1a64 $ TE.encodeUtf8 tplText)
        in do
        srcBytes <- Lbs.readFile sourcePath
        srcText <- either (throwIO . SourceDecodeError   . T.pack . show) pure (TE.decodeUtf8' . Lbs.toStrict $ srcBytes)

        -- putStrLn $ "Template name: " <> T.unpack templateName
        putStrLn $ "Source name: " <> T.unpack sourceName

        -- Upload raw assets to S3
        srcLoc <- ctxt.s3RepoCT srcBytes
        putStrLn $ "Template locator: " <> Uu.toString tplLoc
        putStrLn $ "Source locator: " <> Uu.toString srcLoc

        -- Parse ginger template (from memory).
        -- Ginger's Source type is String; we keep filenames for better error messages.
        tplRez <- parseGinger (s3Resolver ctxt.pgPoolCT ctxt.s3FetchCT) (Just (T.unpack templateName)) (T.unpack tplText)
        case tplRez of
          Left err -> pure . Left . GingerParseError . T.pack . show $ err
          Right template ->
            case parseSourceItems srcText of
              Left err -> pure . Left . SourceParseError . T.pack . show $ err
              Right items ->
                let
                  tplSize = fromIntegral $ T.length tplText
                  srcSize = Lbs.length srcBytes
                in do
                -- putStrLn $ "@[ingestTemplate] proc tpl: [" <> T.unpack (T.take 30 tplText <> "..." <> T.takeEnd 30 tplText) <> "]"
                case prodOpts.dryRunIG of
                  Just True -> do
                    forM_ (zip [1..] items) $ \(idx, item) ->
                      let
                        rendered = renderRequest productionName idx item template
                      in
                      putStrLn $ "@[ingestTemplate] rendered: [" <> T.unpack rendered <> "]"
                    pure $ Right (templateId, cacheKey)
                  _ -> do
                    result <- execStmt "ingestTemplate" ctxt.pgPoolCT $ do
                      -- Add source info:
                      Tx.statement (srcLoc, "source" :: Text, srcSize) Ts.insertS3Object
                      sourceId <- Tx.statement (sourceName,   srcLoc) Ts.insertSource
                      -- Create production row:
                      productionId <- Tx.statement (productionName, templateId, sourceId) Ts.insertProduction

                      -- requests
                      forM_ (zip [1..] items) $ \(idx, item) -> do
                          let
                            rendered = renderRequest productionName idx item template
                          reqId <- Tx.statement (productionId, fromIntegral idx, item.metaJsonSI, rendered) Ts.insertRequest
                          Tx.statement (reqId, "entered" :: Text, mkEnteredDetails templateName sourceName idx item) Ts.insertRequestEvent
                          pure reqId

                      pure productionId

                    case result of
                      Left err -> pure . Left $ DbError (T.pack (show err))
                      Right anID -> pure $ Right (anID, cacheKey)


-- Correct the return values in s3Resolver to match IncludeResolver IO type (i.e., IO (Either Text Text))
s3Resolver :: Pool -> (Text -> IO (Either String Lbs.ByteString)) -> IncludeResolver IO
s3Resolver dbPool s3FetchCT srcName = do
  putStrLn $ "@[s3Resolver] srcName: " <> srcName
  let
    templateName = drop 2 srcName
  eiMbEid <- use dbPool $ Hs.statement (T.pack templateName) Ts.fetchTemplate
  case eiMbEid of
    Left err -> do
      putStrLn $ "@[s3Resolver] template " <> templateName <> " has db err: " <> show err
      pure Nothing -- . Left $ "@[s3Resolver] template :" <> srcName <> " has db err: " <> show err
    Right mbTplEid -> case mbTplEid of
      Nothing -> do
        putStrLn $ "@[s3Resolver] template " <> templateName <> " not found"
        pure Nothing -- . Left $ "@[s3Resolver] template not found: " <> srcName
      Just (templateId, tplLoc) -> do
        eiBytes <- s3FetchCT . T.pack . Uu.toString $ tplLoc
        case eiBytes of
          Left fetchErr -> do
            putStrLn $ "@[s3Resolver] template " <> templateName <> " s3Fetch failed: " <> fetchErr
            pure Nothing -- . Left $ "@[s3Resolver] template s3Fetch failed: " <> fetchErr
          Right bytes -> pure . Just . T.unpack . TE.decodeUtf8 . Lbs.toStrict $ bytes


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
      , "text" ~> sItem.bodySI
      , "meta" ~> (rawJSONToGVal sItem.metaJsonSI :: GVal RenderM)
      , "header_toml" ~> sItem.headerTomlSI
      , "item" ~> (dict [
            "index" ~> ix
          , "content" ~> sItem.bodySI
          , "meta" ~> (rawJSONToGVal sItem.metaJsonSI :: GVal RenderM)
          , "header_toml" ~> sItem.headerTomlSI
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
    , "header_toml"   .= sItem.headerTomlSI
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
                                { headerTomlSI = hdrTxt
                                , metaJsonSI   = meta
                                , bodySI       = bodyTxt
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
