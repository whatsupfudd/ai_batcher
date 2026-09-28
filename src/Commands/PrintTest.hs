module Commands.PrintTest (printCmd) where

import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import Text.Printf (printf)

import Data.Aeson (FromJSON(..), (.:), (.:?), (.!=), eitherDecodeStrict', withObject)

import Options.Cli (PrintTestOpts (..))
import Options.Runtime (RunOptions)


------------------------------------------------------------------------
-- OpenAI response types

data OpenAIResponse = OpenAIResponse
  { modelOAR    :: Text
  , metadataOAR :: ResponseMetadata
  , outputOAR   :: [OutputItem]
  , usageOAR    :: Usage
  }
  deriving (Show)


data ResponseMetadata = ResponseMetadata
  { imageDetailRM :: Maybe Text
  }
  deriving (Show)


data OutputItem = OutputItem
  { contentOI :: [OutputContent]
  }
  deriving (Show)


data OutputContent = OutputContent
  { textOC :: Maybe Text
  }
  deriving (Show)


data Usage = Usage
  { inputTokensU         :: Int
  , inputTokenDetailsU   :: InputTokenDetails
  , outputTokensU        :: Int
  , outputTokenDetailsU  :: OutputTokenDetails
  , totalTokensU         :: Int
  }
  deriving (Show)


data InputTokenDetails = InputTokenDetails
  { cacheWriteTokensITD :: Int
  , cachedTokensITD     :: Int
  }
  deriving (Show)


data OutputTokenDetails = OutputTokenDetails
  { reasoningTokensOTD :: Int
  }
  deriving (Show)


------------------------------------------------------------------------
-- Aeson decoding

instance FromJSON OpenAIResponse where
  parseJSON =
    withObject "OpenAIResponse" $ \o ->
      OpenAIResponse
        <$> o .: "model"
        <*> o .:? "metadata" .!= ResponseMetadata Nothing
        <*> o .: "output"
        <*> o .: "usage"


instance FromJSON ResponseMetadata where
  parseJSON =
    withObject "ResponseMetadata" $ \o -> do
      imageDetail <- o .:? "image_detail"
      -- This fallback lets older/ad-hoc result files use "detail".
      detail <-
        case imageDetail of
          Just value -> pure $ Just value
          Nothing -> o .:? "detail"

      pure $ ResponseMetadata { imageDetailRM = detail }


instance FromJSON OutputItem where
  parseJSON =
    withObject "OutputItem" $ \o ->
      OutputItem <$> o .: "content"


instance FromJSON OutputContent where
  parseJSON =
    withObject "OutputContent" $ \o ->
      OutputContent <$> o .:? "text"


instance FromJSON Usage where
  parseJSON =
    withObject "Usage" $ \o ->
      Usage
        <$> o .: "input_tokens"
        <*> o .:? "input_tokens_details" .!= InputTokenDetails 0 0
        <*> o .: "output_tokens"
        <*> o .:? "output_tokens_details" .!= OutputTokenDetails 0
        <*> o .: "total_tokens"


instance FromJSON InputTokenDetails where
  parseJSON =
    withObject "InputTokenDetails" $ \o ->
      InputTokenDetails
        <$> o .:? "cache_write_tokens" .!= 0
        <*> o .:? "cached_tokens" .!= 0


instance FromJSON OutputTokenDetails where
  parseJSON =
    withObject "OutputTokenDetails" $ \o ->
      OutputTokenDetails <$> o .:? "reasoning_tokens" .!= 0


printCmd :: PrintTestOpts -> RunOptions -> IO ()
printCmd opts runOpts = do
  input <- TIO.readFile opts.inFile
  let
    response = responseToMarkdown input
  TIO.writeFile opts.outFile response
------------------------------------------------------------------------
-- Public conversion function

-- | Convert the JSON returned by the OpenAI Responses API into a
-- Markdown document.
--
-- JSON string escaping is removed automatically by Aeson during
-- decoding.  Thus "\\n" in the JSON source becomes a real newline in
-- the returned Text.
responseToMarkdown :: Text -> Text
responseToMarkdown input =
  case eitherDecodeStrict' (TE.encodeUtf8 input) of
    Left err ->
      renderDecodeError err

    Right response ->
      renderResponse response


------------------------------------------------------------------------
-- Rendering

renderResponse :: OpenAIResponse -> Text
renderResponse response =
  let
    usage = response.usageOAR
    inputDetails = usage.inputTokenDetailsU
    outputDetails = usage.outputTokenDetailsU
    imageDetail = maybe "not recorded" id response.metadataOAR.imageDetailRM
    content = case firstOutputText response of
      Just txt -> txt
      Nothing -> "_No `output[0].content[0].text` value was found._"
    pricing = case estimatedCost usage <$> pricingFor response.modelOAR of
      Just p -> T.pack $ printf "%.8f usd\n" p
      Nothing -> "<?>"
  in
    T.unlines
      [ "# Image Analysis"
      , ""
      , "- **Model:** `" <> modelOAR response <> "`"
      , "- **Image quality:** `" <> imageDetail <> "`"
      , "- **Input tokens:** " <> tshow (inputTokensU usage)
      , "  - Cache-write tokens: " <> tshow (cacheWriteTokensITD inputDetails)
      , "  - Cached tokens: " <> tshow (cachedTokensITD inputDetails)
      , "- **Output tokens:** " <> tshow (outputTokensU usage)
      , "  - Reasoning tokens: " <> tshow (reasoningTokensOTD outputDetails)
      , "- **Total tokens:** " <> tshow (totalTokensU usage)
      , "- **Estimated cost:** " <> pricing
      , ""
      , "---"
      , ""
      , content
      ]


------------------------------------------------------------------------
-- Extract output[0].content[0].text

firstOutputText :: OpenAIResponse -> Maybe Text
firstOutputText response =
  case outputOAR response of
    firstOutput : _ ->
      case contentOI firstOutput of
        firstContent : _ -> textOC firstContent
        [] -> Nothing
    [] -> Nothing


------------------------------------------------------------------------
-- Errors/utilities

renderDecodeError :: String -> Text
renderDecodeError err =
  T.unlines
    [ "# Image Analysis"
    , ""
    , "**Error:** Could not decode the OpenAI response JSON."
    , ""
    , "```text"
    , T.pack err
    , "```"
    ]


tshow :: Show a => a -> Text
tshow = T.pack . show

--- TODO:  Deduplicate from ImgTest.hs
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


estimatedCost :: Usage -> Pricing -> Double
estimatedCost usage pricing =
  let
    cached = usage.inputTokenDetailsU.cachedTokensITD
    uncached = max 0 $ usage.inputTokensU - cached
    inputCost = fromIntegral uncached * pricing.inputPriceP / 1000000.0
    cachedCost = fromIntegral cached * pricing.cachedPriceP / 1000000.0
    cacheWriteCost = fromIntegral usage.inputTokenDetailsU.cacheWriteTokensITD * pricing.inputPriceP / 1000000.0
    outputCost = fromIntegral usage.outputTokensU * pricing.outputPriceP / 1000000.0
  in
    inputCost + cachedCost + cacheWriteCost + outputCost
