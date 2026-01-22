{-# LANGUAGE DerivingStrategies #-}

module Options.Cli where

import Data.Text (Text)
import Options.Applicative


newtype EnvOptions = EnvOptions {
    appHome :: Maybe FilePath
  }

data CliOptions = CliOptions {
  debug :: Maybe Int
  , configFile :: Maybe FilePath
  , job :: Maybe Command
 }
 deriving stock (Show)

data GlobalOptions = GlobalOptions {
  confPathGO :: String
  , debugGO :: String
  }

data ProducerOpts = ProducerOpts {
  templateIG :: FilePath
  , sourceIG :: FilePath
  , productIG :: Text
  , versionIG :: Maybe Text
  }
  deriving (Show)

data Command =
  HelpCmd
  | VersionCmd
  | ExtractCmd FilePath FilePath
  | FetchCmd FilePath FilePath
  | LoadCmd FilePath FilePath FilePath
  | PostPCmd FilePath
  | GenDocsCmd FilePath FilePath
  | ServerCmd
  | ProducerCmd ProducerOpts
  deriving stock (Show)

parseCliOptions :: IO (Either String CliOptions)
parseCliOptions =
  Right <$> execParser parser

parser :: ParserInfo CliOptions
parser =
  info (helper <*> argumentsP) $
    fullDesc <> progDesc "docproc." <> header "docproc - ."


argumentsP :: Parser CliOptions
argumentsP = do
  buildOptions <$> globConfFileDef <*> hsubparser commandDefs
  where
    buildOptions :: GlobalOptions -> Command -> CliOptions
    buildOptions globs cmd =
      let
        mbConfPath = case globs.confPathGO of
          "" -> Nothing
          aValue -> Just aValue
        mbDebug = case globs.debugGO of
          "" -> Nothing
          aValue -> Just (read aValue :: Int)
      in
      CliOptions {
        debug = mbDebug
        , configFile = mbConfPath
        , job = Just cmd
      }


globConfFileDef :: Parser GlobalOptions
globConfFileDef =
  GlobalOptions <$>
    strOption (
      long "config"
      <> short 'c'
      <> metavar "docprocCONF"
      <> value ""
      <> showDefault
      <> help "Global config file (default is ~/.docproc/config.yaml)."
    )
    <*>
    strOption (
      long "debug"
      <> short 'd'
      <> metavar "DEBUGLVL"
      <> value ""
      <> showDefault
      <> help "Global debug state."
    )
  

commandDefs :: Mod CommandFields Command
commandDefs =
  let
    cmdArray = [
      ("help", pure HelpCmd, "Help about any command.")
      , ("version", pure VersionCmd, "Shows the version number of importer.")
      , ("extract", extractOpts, "Extracts the text from a JSONL file.")
      , ("fetch", fetchOpts, "Fetches the results from a JSONL file.")
      , ("load", loadOpts, "Loads the results into a JSONL file.")
      , ("postp", postpOpts, "Posts the results to a HTML file.")
      , ("gendoc", gendocOpts, "Generates DOCX and PDF files from a HTML file.")
      , ("server", pure ServerCmd, "Starts the server.")
      , ("producer", ProducerCmd <$> producerOpts, "Produce a new product version from a template and source file.")
      ]
    headArray = head cmdArray
    tailArray = tail cmdArray
  in
    foldl (\accum aCmd -> cmdBuilder aCmd <> accum) (cmdBuilder headArray) tailArray
  where
    cmdBuilder (label, cmdDef, desc) =
      command label (info cmdDef (progDesc desc))

extractOpts :: Parser Command
extractOpts =
  ExtractCmd <$> strArgument (metavar "JSONL_FILE" <> help "JSONL file to extract text from.")
    <*> strArgument (metavar "OUTPUT_PREFIX" <> help "Prefix for output files.")

fetchOpts :: Parser Command
fetchOpts =
  FetchCmd <$> strArgument (metavar "OUT_FILE" <> help "Output file to fetch results from.")
    <*> strArgument (metavar "RESULTS_DIR" <> help "Directory to write results to.")

loadOpts :: Parser Command
loadOpts =
  LoadCmd <$> strArgument (metavar "DOC_FILE" <> help "Document file to load.")
    <*> strArgument (metavar "PROMPTS_FILE" <> help "Prompts file to load.")
    <*> strArgument (metavar "OUT_FILE" <> help "Output file to write results to.")

postpOpts :: Parser Command
postpOpts =
  PostPCmd <$> strArgument (metavar "HTML_FILE" <> help "HTML file to postprocess.")

gendocOpts :: Parser Command
gendocOpts =
  GenDocsCmd <$> strArgument (metavar "HTML_FILE" <> help "HTML file to generate DOCX and PDF files from.")
    <*> strArgument (metavar "OUTPUT_PREFIX" <> help "Prefix for output files.")

producerOpts :: Parser ProducerOpts
producerOpts =
  ProducerOpts <$> strArgument (metavar "TEMPLATE_FILE" <> help "Template file to use for production.")
    <*> strArgument (metavar "SOURCE_FILE" <> help "Source file to use for production.")
    <*> strArgument (metavar "PRODUCTION_NAME" <> help "Name to use for production.")
    <*> optional (strArgument (metavar "VERSION" <> help "Version to produce."))