-- gendoc — Convert postp-produced HTML to DOCX and PDF
-- First draft for CTO review.
--
-- Strategy:
--   • Use Pandoc (Haskell lib) to read HTML → Pandoc AST
--   • Write DOCX via Pandoc's docx writer (pure Haskell)
--   • Write PDF via Pandoc's PDF pipeline (requires an external pdf engine like xelatex);
--     we invoke Text.Pandoc.PDF.makePDF from the library. Optionally override engine
--     with env PDF_ENGINE (e.g., "xelatex", "tectonic", "wkhtmltopdf", "weasyprint").
--
-- CLI:
--   html2doc <input.html> [output_prefix]
--     If output_prefix omitted, derive from input path without extension.
--
-- ENV:
--   PDF_ENGINE=xelatex         # default if unset
--   REFERENCE_DOCX=path.docx   # optional reference docx to style the Word output
--   PDF_TEMPLATE=path.latex    # optional custom LaTeX template
--
-- Build deps (package.yaml): pandoc >= 3, pandoc-types, pandoc-cli (optional),
-- bytestring, text, directory, filepath, process (for optional fallback).

module Commands.GenDocs (doGenDocs) where

import Control.Monad (unless)
import qualified Data.ByteString.Lazy  as LBS
import qualified Data.Map as Mp
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.Environment (getArgs, lookupEnv)
import System.Exit (exitFailure)
import System.FilePath (dropExtension, takeDirectory)
import System.Directory (doesFileExist)
import System.Process (readProcess)

import Text.Pandoc ( def
  , ReaderOptions(..)
  , MetaValue(..)
  , readerExtensions
  , pandocExtensions
  , runIOorExplode
  , readHtml
  , writeDocx
  )
import qualified Text.Pandoc as P
import qualified Text.Pandoc.Builder as Pb
import qualified Text.Pandoc.PDF as PDF
import qualified Text.DocTemplates as Pt 

import qualified Options as Opt


doGenDocs :: FilePath -> FilePath -> Opt.RunOptions -> IO ()
doGenDocs inHtml outPrefix rtOpts = do
  ok <- doesFileExist inHtml
  unless ok $ do
    putStrLn $ "[FATAL] File not found: " ++ inHtml
    exitFailure

  html <- TIO.readFile inHtml

  -- Reader opts: enable full pandoc extensions for best HTML fidelity
  let ropts = def { readerExtensions = pandocExtensions }

  doc <- runIOorExplode $ readHtml ropts html
  let
    doc' = addHyperrefHeader doc

  -- DOCX ----------------------------------------------------------------------
  mRef <- lookupEnv "REFERENCE_DOCX"
  let
    woptsDocx = def { P.writerReferenceDoc = mRef }
    docxPath  = outPrefix ++ ".docx"

  docxBS <- runIOorExplode $ writeDocx woptsDocx doc
  LBS.writeFile docxPath docxBS
  putStrLn $ "Wrote DOCX: " ++ docxPath

  -- PDF -----------------------------------------------------------------------
  -- Choose pdf engine; default xelatex. Optional custom LaTeX template path.
  engine <- fmap (fromMaybe "xelatex") (lookupEnv "PDF_ENGINE")
  mTplPath <- lookupEnv "PDF_TEMPLATE"

-- compile template if provided
  tplOpt <- case mTplPath of
    Nothing -> do
      defaultTemplate <- P.runIOorExplode $ P.getDefaultTemplate "latex"
      eiTmpl <- P.compileTemplate "user-template" defaultTemplate
      case eiTmpl of
        Left err -> do
          putStrLn $ "[WARN] Could not compile PDF template: " <> show err
          pure Nothing                         -- fall back to default template
        Right tpl -> pure (Just tpl)
    Just path -> do
      src <- TIO.readFile path
      eiTmpl <- P.compileTemplate "user-template" src               -- template source as Text
      case eiTmpl of
        Left err -> do
          putStrLn $ "[WARN] Could not compile PDF template: " <> show err
          pure Nothing                         -- fall back to default template
        Right tpl -> pure (Just tpl)

  mMain <- lookupEnv "MAIN_FONT"   -- e.g. "STIX Two Text" or "Noto Serif"
  mMath <- lookupEnv "MATH_FONT"   -- e.g. "STIX Two Math" or "TeX Gyre Termes Math"

  let 
    mainFont = T.pack (fromMaybe "TeX Gyre Termes" mMain)
    mathFont = T.pack (fromMaybe "TeX Gyre Termes Math" mMath)

-- writer options for LaTeX → PDF
  let 
    woptsPdfB :: P.WriterOptions
    woptsPdfB = P.def
      { P.writerExtensions = P.pandocExtensions
      , P.writerTemplate = tplOpt
      , P.writerVariables = Pt.toContext $ Mp.fromList [ -- ("mainfont", mainFont)          -- triggers \usepackage{fontspec} + \setmainfont
           ("mathfont", mathFont)          -- triggers \usepackage{unicode-math} + \setmathfont
          , ("linkcolor", "black") :: (T.Text, T.Text)        -- optional cosmetics
          , ("geometry","margin=1in")
          , ("colorlinks","true")
          ]
      }
    pdfPath  = outPrefix ++ ".pdf"
  woptsPdf <- mkWoptsPdfXe tplOpt


-- (optional) dump intermediate .tex if you need to debug:
  tex <- P.runIOorExplode $ P.writeLaTeX woptsPdf doc'
  TIO.writeFile (outPrefix <> ".tex") tex

-- build the PDF
  let engArgs = ["-halt-on-error"]
  eiPdfBytes <- P.runIOorExplode $ PDF.makePDF engine engArgs P.writeLaTeX woptsPdf doc'

  case eiPdfBytes of
    Left err -> do
      putStrLn $ "[ERROR] PDF generation failed: " ++ show err
      exitFailure
    Right pdfBytes -> do
      LBS.writeFile pdfPath pdfBytes
  putStrLn $ "Wrote PDF:  " ++ pdfPath


-- Ensure hyperref is loaded via Pandoc metadata ("header-includes").
-- If header-includes already exists, we append to it; otherwise we create it.
addHyperrefHeader :: P.Pandoc -> P.Pandoc
addHyperrefHeader (P.Pandoc meta blocks) =
  let newEntries :: [MetaValue]
      newEntries =
        [ P.MetaBlocks [ P.RawBlock (P.Format "latex") "\\usepackage{hyperref}" ]
        , P.MetaBlocks [ P.RawBlock (P.Format "latex") "\\hypersetup{hidelinks}" ]
        ]

      merged :: MetaValue
      merged =
        case P.lookupMeta "header-includes" meta of
          Just (P.MetaList xs) -> P.MetaList (xs <> newEntries)
          Just mv -> P.MetaList ([mv] <> newEntries)
          Nothing -> P.MetaList newEntries

      meta' = Pb.setMeta "header-includes" merged meta
  in P.Pandoc meta' blocks

findTLFont :: FilePath -> IO (Maybe FilePath)
findTLFont fileName = do
  -- returns full path or empty
  out <- readProcess "kpsewhich" [fileName] ""
  let p = T.unpack (T.strip (T.pack out))
  pure $ if null p then Nothing else Just p


-- Build writer options for xelatex, preferring TeX Gyre from TeX Live.
mkWoptsPdfXe :: Maybe (P.Template T.Text) -> IO P.WriterOptions
mkWoptsPdfXe aTemplate = do
  -- TeX Gyre Termes (text) faces
  mReg <- findTLFont "texgyretermes-regular.otf"
  mBol <- findTLFont "texgyretermes-bold.otf"
  mIt  <- findTLFont "texgyretermes-italic.otf"
  mBi  <- findTLFont "texgyretermes-bolditalic.otf"
  -- TeX Gyre Termes Math
  mMath <- findTLFont "texgyretermes-math.otf"

  case (mReg, mBol, mIt, mBi, mMath) of
    (Just reg, Just bol, Just it, Just bi, Just mth) -> do
      let dir   = takeDirectory reg   -- same dir for all
          -- IMPORTANT: use base filenames with Path+Extension
          mfOpts = T.intercalate "," 
                    [ "Path=" <> T.pack dir <> "/"
                    , "Extension=.otf"
                    , "UprightFont=texgyretermes-regular"
                    , "BoldFont=texgyretermes-bold"
                    , "ItalicFont=texgyretermes-italic"
                    , "BoldItalicFont=texgyretermes-bolditalic"
                    ]
          mmOpts = T.intercalate ","
                    [ "Path=" <> T.pack (takeDirectory mth) <> "/"
                    , "Extension=.otf"
                    ]
      pure P.def
        { P.writerExtensions = P.pandocExtensions
        , P.writerTemplate = aTemplate
        , P.writerVariables = Pt.toContext $ Mp.fromList 
            [ ("mainfont"       , "texgyretermes-regular")  -- base filename, not display name
            , ("mainfontoptions", mfOpts)
            , ("mathfont"       , "texgyretermes-math")     -- base filename, not display name
            , ("mathfontoptions", mmOpts)
            , ("linkcolor","black") :: (T.Text, T.Text)
            ]
        }
    _ -> do
      -- Fallback to system fonts (install via brew cask if needed)
      let mainFont = "Noto Serif" :: T.Text
          mathFont = "STIX Two Math" :: T.Text
      pure P.def
        { P.writerExtensions = P.pandocExtensions
        , P.writerTemplate = Nothing
        , P.writerVariables = Pt.toContext $ Mp.fromList 
            [ ("mainfont", mainFont)
            , ("mathfont", mathFont)
            , ("linkcolor","black") :: (T.Text, T.Text)
            ]
        }
