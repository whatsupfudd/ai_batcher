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
  templateNameIG :: Text
  , newTemplateSrcIG :: Maybe FilePath
  , sourceIG :: FilePath
  , productIG :: Maybe Text
  , versionIG :: Maybe Text
  , providerIG :: Maybe Text
  , modelIG :: Maybe Text
  , dryRunIG :: Maybe Bool
  , batchSizeIG :: Maybe Int
  }
  deriving (Show)

newtype ReceiverOpts = ReceiverOpts {
  providerRC :: Text
  }
  deriving (Show)

data ProcessOpts = ProcessOpts {
  productionNamePC :: Text
  , outputModePC :: Text
  , outputDirPC :: Maybe FilePath
  }
  deriving (Show)

data TemplateOpts = TemplateOpts {
    fileName :: FilePath
  , uName :: Text
  } deriving (Show)

data SubmitOpts = SubmitOpts {
  inputFileName :: FilePath
  , templateID :: Text
  , serviceID :: Maybe Text
  , modelName :: Maybe Text
  } deriving (Show)


data TemplateSubCmds =
  LoadTP LoadTO
  | ListTP ListTO
  | DeleteTP DeleteTO
  deriving (Show)

data LoadTO = LoadTO {
  loadTemplateFileName :: FilePath
  , loadTemplateID :: Text
  } deriving (Show)

newtype ListTO = ListTO {
  listTemplateFilter :: Maybe Text
  } deriving (Show)

newtype DeleteTO = DeleteTO {
  deleteTemplateID :: Text
  } deriving (Show)

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
  | ReceiverCmd ReceiverOpts
  | ProcessCmd ProcessOpts
  | TemplateCmd TemplateSubCmds
  | SubmitCmd SubmitOpts
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
  

cmdBuilder :: (String, Parser a, String) -> Mod CommandFields a
cmdBuilder (label, cmdDef, desc) =
  command label (info cmdDef (progDesc desc))


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
      -- New commands:
      , ("producer", ProducerCmd <$> producerOpts, "Produce a new product version from a template and source file.")
      , ("receiver", ReceiverCmd <$> receiverOpts, "Receives results from providers.")
      , ("process", ProcessCmd <$> processOpts, "Processes the results.")
      , ("template", TemplateCmd <$> templateSubCmds, "Manages template information into the database.")
      , ("submit", SubmitCmd <$> submitOpts, "Submits a set of items to a provider through the templating engine.")
      ]
    headArray = head cmdArray
    tailArray = tail cmdArray
  in
    foldl (\accum aCmd -> cmdBuilder aCmd <> accum) (cmdBuilder headArray) tailArray


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
  ProducerOpts
    <$> strArgument (metavar "TEMPLATE_NAME" <> help "Name of template to use for production.")
    <*> optional (strOption (long "template" <> short 't' <> metavar "TEMPLATE_FILE" <> help "Template file to use for production."))
    <*> strArgument (metavar "SOURCE_FILE" <> help "Source file to use for production.")
    <*> optional (strArgument (metavar "PRODUCTION_NAME" <> help "Name to use for production."))
    <*> optional (strArgument (metavar "VERSION" <> help "Version to produce."))
    <*> optional (strOption (long "provider" <> short 'p' <> metavar "PROVIDER" <> help "Provider to use for production."))
    <*> optional (strOption (long "model" <> short 'm' <> metavar "MODEL" <> help "Model to use for production."))
    <*> optional (flag False True (long "dry-run" <> short 'n' <> help "Dry run mode."))
    <*> optional (option auto (long "batch-size" <> short 'b' <> metavar "BATCH_SIZE" <> help "Batch size to use for production."))


receiverOpts :: Parser ReceiverOpts
receiverOpts =
  ReceiverOpts
    <$> strArgument (metavar "SERVICE_PROVIDER" <> help "Service provider to receive results from.")

processOpts :: Parser ProcessOpts
processOpts =
  ProcessOpts
    <$> strArgument (metavar "PRODUCTION_NAME" <> help "Name of production to process.")
    <*> strArgument (metavar "OUTPUT_MODE" <> help "Output mode to use for processing (code, docx, htmljs, text).")
    <*> optional (strOption (long "output-dir" <> short 'o' <> metavar "OUTPUT_DIR" <> help "Directory to write output to."))


templateSubCmds :: Parser TemplateSubCmds
templateSubCmds =
  let
    cmdArray = [
        ("load", LoadTP <$> loadTemplateOpts, "Loads template information into the database.")
        , ("list", ListTP <$> listTemplateOpts, "Lists template information from the database.")
        , ("delete", DeleteTP <$> deleteTemplateOpts, "Deletes a template from the database.")
      ]
    headArray = head cmdArray
    tailArray = tail cmdArray
  in
    subparser $ foldl (\accum aCmd -> cmdBuilder aCmd <> accum) (cmdBuilder headArray) tailArray


loadTemplateOpts :: Parser LoadTO
loadTemplateOpts =
  LoadTO <$> strArgument (metavar "TEMPLATE_FILE_NAME" <> help "Template file name to load.")
    <*> strArgument (metavar "TEMPLATE_ID" <> help "Template ID to assign to the file.")

listTemplateOpts :: Parser ListTO
listTemplateOpts =
  ListTO <$> optional (strOption (long "filter" <> short 'f' <> metavar "TEMPLATE_FILTER" <> help "Filter to apply to the list search."))
  
deleteTemplateOpts :: Parser DeleteTO
deleteTemplateOpts =
  DeleteTO <$> strArgument (metavar "TEMPLATE_ID" <> help "Template ID to delete.")



submitOpts :: Parser SubmitOpts
submitOpts =
  SubmitOpts <$> strArgument (metavar "INPUT_FILE_NAME" <> help "Input file name to submit.")
    <*> strArgument (metavar "TEMPLATE_ID" <> help "Template ID to use to assemble prompts.")
    <*> optional (strOption (long "service-id" <> short 's' <> metavar "SERVICE_ID" <> help "Service ID to submit."))
    <*> optional (strOption (long "model-name" <> short 'm' <> metavar "MODEL_NAME" <> help "Model name to submit."))