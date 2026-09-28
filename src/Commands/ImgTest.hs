module Commands.ImgTest where

import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Control.Exception (SomeException, try)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as Lbs
import Data.Char (toLower)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time.Clock
  ( diffUTCTime
  , getCurrentTime
  )
import qualified Data.Vector as V

import Data.Aeson ((.:), (.:?), (.=))
import qualified Data.Aeson as A
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.Aeson.Types as AT

import Network.HTTP.Client
  ( Manager
  , Request(..)
  , RequestBody(..)
  , Response
  , httpLbs
  , newManager
  , parseRequest
  , responseBody
  , responseStatus
  , managerResponseTimeout, responseTimeoutMicro
  )
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Status (statusCode)
import System.Environment (lookupEnv)
import System.Exit (die)
import System.FilePath (dropExtension, takeExtension)
import Text.Printf (printf)

import Options.Runtime (RunOptions (..))
import Options.Cli (ImgTestOpts (..))


imgTest :: ImgTestOpts -> RunOptions -> IO ()
imgTest opts runOpts = do
  doTest opts.imagePathIG opts.modelIG opts.detailIG
  {-
  putStrLn $ unlines [
       "Usage:"
    , "  image-analysis-test IMAGE [MODEL] [DETAIL]"
    , ""
    , "Examples:"
    , "  image-analysis-test painting.jpg"
    , "  image-analysis-test painting.jpg gpt-5.6-luna high"
    , "  image-analysis-test painting.jpg gpt-5.6-terra original"
    , ""
    , "DETAIL:"
    , "  low | high | original | auto"
    ]
  -}

------------------------------------------------------------------------
-- Configuration

defaultModel :: Text
defaultModel = "gpt-5.6-luna"


defaultDetail :: Text
defaultDetail = "high"


defaultPrompt :: Text
defaultPrompt =
  T.unlines
    [ "Analyse the supplied image in detail."
    , ""
    , "Describe only what can reasonably be inferred from visible evidence."
    , "Be comprehensive but factual."
    , ""
    , "Include:"
    , "- a concise overall summary;"
    , "- the principal objects or subjects;"
    , "- important attributes of those objects;"
    , "- actions and spatial relationships;"
    , "- setting and background;"
    , "- composition, viewpoint and framing;"
    , "- lighting and visually important colours;"
    , "- artistic or photographic style where applicable;"
    , "- all clearly legible visible text, transcribed verbatim;"
    , "- unusual or especially salient details;"
    , "- uncertainties where the visual evidence is ambiguous."
    , ""
    , "Do not invent details that cannot be seen."
    ]


------------------------------------------------------------------------
-- Usage information

data Usage = Usage
  { inputTokensU     :: Int
  , cachedTokensU    :: Int
  , outputTokensU    :: Int
  , reasoningTokensU :: Int
  , totalTokensU     :: Int
  }
  deriving (Show)


parseUsage :: A.Value -> Either String Usage
parseUsage =
  AT.parseEither $ A.withObject "response" $ \root -> do
      usageVal <- root .: "usage"
      A.withObject "usage" parseUsageObject usageVal
  where
    parseUsageObject o = do
      inputTokens <- o .: "input_tokens"
      outputTokens <- o .: "output_tokens"
      totalTokens <- o .: "total_tokens"
      inputDetails <- o .:? "input_tokens_details" :: AT.Parser (Maybe A.Value)
      outputDetails <- o .:? "output_tokens_details" :: AT.Parser (Maybe A.Value)
      cachedTokens <-
        case inputDetails of
          Just v -> A.withObject "input_tokens_details" (\x -> fromMaybe 0 <$> x .:? "cached_tokens") v
          Nothing -> pure 0
      reasoningTokens <-
        case outputDetails of
          Just v -> A.withObject "output_tokens_details" (\x -> fromMaybe 0 <$> x .:? "reasoning_tokens") v
          Nothing -> pure 0

      pure Usage
        { inputTokensU = inputTokens
        , cachedTokensU = cachedTokens
        , outputTokensU = outputTokens
        , reasoningTokensU = reasoningTokens
        , totalTokensU = totalTokens
        }


------------------------------------------------------------------------
-- Response information

data ResponseInfo = ResponseInfo
  { idRI     :: Text
  , modelRI  :: Text
  , statusRI :: Text
  }
  deriving (Show)


parseResponseInfo :: A.Value -> Either String ResponseInfo
parseResponseInfo =
  AT.parseEither $ A.withObject "response" $ \o -> ResponseInfo <$> o .: "id" <*> o .: "model" <*> o .: "status"


------------------------------------------------------------------------
-- Text extraction

extractOutputText :: A.Value -> Either String Text
extractOutputText =
  AT.parseEither $
    A.withObject "response" $ \root -> do
      outputs <- root .: "output" :: AT.Parser [A.Value]
      chunks <- fmap concat $ mapM parseOutput outputs
      pure $ T.intercalate "\n\n" . filter (not . T.null) . map T.strip $ chunks
  where
    parseOutput :: A.Value -> AT.Parser [Text]
    parseOutput =
      A.withObject "output item" $ \o -> do
        outputType <- o .:? "type" :: AT.Parser (Maybe Text)
        case outputType of
          Just "message" -> do
            mbContent <- o .:? "content" :: AT.Parser (Maybe [A.Value])
            case mbContent of
              Nothing -> pure []
              Just content -> fmap concat $ mapM parseContent content
          _ -> pure []

    parseContent :: A.Value -> AT.Parser [Text]
    parseContent =
      A.withObject "content item" $ \o -> do
        mbText <- o .:? "text" :: AT.Parser (Maybe Text)
        pure $ maybe [] pure mbText


------------------------------------------------------------------------
-- Image input

mimeTypeFor :: FilePath -> Either String Text
mimeTypeFor path =
  case map toLower (takeExtension path) of
    ".jpg" -> Right "image/jpeg"
    ".jpeg" -> Right "image/jpeg"
    ".png" -> Right "image/png"
    ".webp" -> Right "image/webp"
    extension -> Left $ "Unsupported image extension: " <> extension <> ". Use JPEG, PNG or WebP for this test."


makeDataUrl :: FilePath -> IO (Text, Int)
makeDataUrl path = do
  mime <- either die pure $ mimeTypeFor path
  bytes <- BS.readFile path
  let
    encoded = B64.encode bytes
    url = "data:" <> mime <> ";base64," <> TE.decodeUtf8 encoded
  pure (url, BS.length bytes)


------------------------------------------------------------------------
-- Request

makePayload :: Text -> Text -> Text -> Text -> A.Value
makePayload model detail imageUrl context =
  A.object
    [ "model" .= model
    , "service_tier" .= ("default" :: Text)
    -- We want to measure vision, not spend tokens on reasoning yet.
    , "reasoning" .= A.object
        [ "effort" .= ("none" :: Text)
        ]
    , "max_output_tokens" .= (4000 :: Int)
    -- Nothing about this experiment needs conversational persistence.
    , "store" .= False
    , "input" .=
        [ A.object
            [ "role" .= ("user" :: Text)
            , "content" .=
                [ A.object [ "type" .= ("input_text" :: Text), "text" .= context ]
                , A.object [ "type" .= ("input_image" :: Text), "image_url" .= imageUrl, "detail" .= detail ]
                ]
            ]
        ]
    ]


sendRequest
  :: Manager
  -> String
  -> A.Value
  -> IO (Response Lbs.ByteString)
sendRequest manager apiKey payload = do
  request0 <-
    parseRequest "https://api.openai.com/v1/responses"

  let
    request =
      request0
        { method = "POST"

        , requestHeaders =
            [ ("Authorization", BS8.pack ("Bearer " <> apiKey))
            , ("Content-Type", "application/json")
            ]

        , requestBody =
            RequestBodyLBS (A.encode payload)

        -- Keep the body on non-2xx responses so that the test tool
        -- can print useful OpenAI error messages.
        , checkResponse =
            \_ _ -> pure ()
        }

  httpLbs request manager


------------------------------------------------------------------------
-- Approximate current synchronous token pricing

data Pricing = Pricing {
    inputPriceP  :: Double
  , cachedPriceP :: Double
  , outputPriceP :: Double
  }


pricingFor :: Text -> Maybe Pricing
pricingFor model
  | "gpt-5.6-luna" `T.isPrefixOf` model = Just $ Pricing 0.20 0.02 1.20
  | "gpt-5.6-terra" `T.isPrefixOf` model = Just $ Pricing 2.00 0.20 12.00
  | "gpt-5.6-sol" `T.isPrefixOf` model
      || model == "gpt-5.6" = Just $ Pricing 4.00 0.40 20.00
  | "gpt-6-luna" `T.isPrefixOf` model = Just $ Pricing 0.10 0.01 0.5
  | "gpt-6-sol" `T.isPrefixOf` model = Just $ Pricing 2.00 0.20 10.00
  | "gpt-6-astra" `T.isPrefixOf` model
      || model == "gpt-6" = Just $ Pricing 10.00 1.00 50.00
  | otherwise = Nothing


estimatedCost :: Pricing -> Usage -> Double
estimatedCost pricing usage =
  let
    cached = cachedTokensU usage
    uncached = max 0 $ inputTokensU usage - cached
    inputCost = fromIntegral uncached * inputPriceP pricing / 1000000.0
    cachedCost = fromIntegral cached * cachedPriceP pricing / 1000000.0
    outputCost = fromIntegral (outputTokensU usage) * outputPriceP pricing / 1000000.0
  in
    inputCost + cachedCost + outputCost


------------------------------------------------------------------------
-- Main

doTest :: FilePath -> Text -> Text -> IO ()
doTest imagePath model detail = do

  if detail `elem` ["low", "high", "original", "auto"] then 
    pure ()
  else
    die "DETAIL must be low, high, original, or auto."

  mbApiKey <- lookupEnv "OPENAI_API_KEY"

  apiKey <-
    case mbApiKey of
      Just key
        | not (null key) -> pure key
      _ -> die "OPENAI_API_KEY is not set."

  (imageUrl, imageBytes) <- makeDataUrl imagePath
  context <- TIO.readFile "Assets/kdofaContext_1.md"

  let
    payload = makePayload model detail imageUrl context

    outputPath =
      dropExtension imagePath
        <> "."
        <> T.unpack model
        <> "."
        <> T.unpack detail
        <> ".response.json"

  let
    mgrSettings = tlsManagerSettings { managerResponseTimeout = responseTimeoutMicro $ 5 * 60000000 }
  manager <- newManager mgrSettings

  putStrLn "OpenAI image-analysis test"
  putStrLn "--------------------------"
  putStrLn $ "Image:       " <> imagePath
  putStrLn $ "Image bytes: " <> show imageBytes
  putStrLn $ "Model:       " <> T.unpack model
  putStrLn $ "Detail:      " <> T.unpack detail
  putStrLn ""

  started <- getCurrentTime
  result <- try $ sendRequest manager apiKey payload
  finished <- getCurrentTime

  let
    elapsed = realToFrac (diffUTCTime finished started) :: Double

  printf "Wall time:   %.3f seconds\n" elapsed

  response <-
    case result of
      Left err -> die $ "HTTP exception: " <> show (err :: SomeException)
      Right value -> pure value

  let
    code = statusCode $ responseStatus response
    body = responseBody response

  putStrLn $ "HTTP status: " <> show code

  value <-
    case A.eitherDecode body of
      Left err -> do
        Lbs.writeFile outputPath body

        die $
          "Could not decode OpenAI response: "
            <> err
            <> "\nRaw body written to "
            <> outputPath

      Right decoded -> pure decoded

  Lbs.writeFile outputPath (AP.encodePretty value)

  if code < 200 || code >= 300 then do
    putStrLn ""
    putStrLn "OpenAI returned an error:"
    Lbs.putStr $ AP.encodePretty value
    putStrLn ""
    die $ "Raw response written to " <> outputPath
  else
    pure ()

  case parseResponseInfo value of
    Left err ->
      putStrLn $
        "Warning: response metadata parse failed: "
          <> err

    Right info -> do
      putStrLn $ "Response id:  " <> T.unpack info.idRI
      putStrLn $ "Status:       " <> T.unpack info.statusRI
      putStrLn $ "Actual model: " <> T.unpack info.modelRI

  putStrLn ""

  mbUsage <-
    case parseUsage value of
      Left err -> do
        putStrLn $
          "Warning: usage parse failed: "
            <> err

        pure Nothing

      Right usage -> do
        putStrLn "Token usage"
        putStrLn "-----------"

        putStrLn $
          "Input tokens:      "
            <> show usage.inputTokensU

        putStrLn $
          "  cached:          "
            <> show usage.cachedTokensU

        putStrLn $
          "Output tokens:     "
            <> show usage.outputTokensU

        putStrLn $
          "  reasoning:       "
            <> show usage.reasoningTokensU

        putStrLn $
          "Total tokens:      "
            <> show usage.totalTokensU

        pure $ Just usage

  case (mbUsage, pricingFor model) of
    (Just usage, Just pricing) -> do
      printf
        "Estimated API cost: $%.8f\n"
        (estimatedCost pricing usage)

    _ ->
      putStrLn $
        "Estimated API cost: unavailable for this model."

  putStrLn ""
  putStrLn "Model output"
  putStrLn "============"

  case extractOutputText value of
    Left err ->
      putStrLn $
        "Could not extract output text: "
          <> err

    Right txt ->
      putStrLn $
        T.unpack txt

  putStrLn ""
  putStrLn $
    "Complete JSON response: "
      <> outputPath