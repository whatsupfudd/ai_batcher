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
import qualified Numeric as Nm
import System.Environment (getArgs, lookupEnv)
import System.Exit (exitFailure)
import System.FilePath (dropExtension, takeDirectory)
import System.Directory (doesFileExist)
import System.Process (readProcess)

import Text.Pandoc ( def
  , Pandoc
  , ReaderOptions(..)
  , MetaValue(..)
  , Meta(..), Block(..)
  , Inline(..), MathType(..), nullAttr
  , readerExtensions
  , pandocExtensions
  , runIOorExplode
  , readHtml
  , writeDocx
  )
import Text.Pandoc.Definition ()
import Text.Pandoc.Walk (walk)

import qualified Text.Pandoc as P
import qualified Text.Pandoc.Builder as Pb
import qualified Text.Pandoc.PDF as PDF
import qualified Text.DocTemplates as Pt 

import qualified Options.Runtime as Opt


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

  let doc'' = ensureUnicodeCoverage . neutralizeProblemMath $ doc'
-- (optional) dump intermediate .tex if you need to debug:
  tex <- P.runIOorExplode $ P.writeLaTeX woptsPdf doc''
  TIO.writeFile (outPrefix <> ".tex") tex

-- build the PDF
  let engArgs = ["-halt-on-error"]
  eiPdfBytes <- P.runIOorExplode $ PDF.makePDF engine engArgs P.writeLaTeX woptsPdf doc''

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

-- Engine-aware Unicode coverage for pdfLaTeX vs Xe/LuaLaTeX.
-- Inserts a header-includes block that:
--  * pdfTeX: \usepackage[utf8]{inputenc} + \DeclareUnicodeCharacter{...}{...}
--  * Xe/Lua: \usepackage{newunicodechar} + \newunicodechar{^^^^XXXX}{...}
ensureUnicodeCoverage :: P.Pandoc -> P.Pandoc
ensureUnicodeCoverage (P.Pandoc meta blocks) =
  let incs = [ raw "\\usepackage{iftex}"
             , raw "\\ifPDFTeX"
             , raw "  \\usepackage[utf8]{inputenc}"
             , raw "  \\usepackage{amsmath,amssymb}"
             , raw "  % Subscript digits U+2080..U+2089"
             , decl "2080" "\\ensuremath{_{0}}"
             , decl "2081" "\\ensuremath{_{1}}"
             , decl "2082" "\\ensuremath{_{2}}"
             , decl "2083" "\\ensuremath{_{3}}"
             , decl "2084" "\\ensuremath{_{4}}"
             , decl "2085" "\\ensuremath{_{5}}"
             , decl "2086" "\\ensuremath{_{6}}"
             , decl "2087" "\\ensuremath{_{7}}"
             , decl "2088" "\\ensuremath{_{8}}"
             , decl "2089" "\\ensuremath{_{9}}"
             , raw "  % Vulgar fraction and arrows/logic"
             , decl "2154" "\\ensuremath{\\tfrac{2}{3}}"
             , decl "2194" "\\ensuremath{\\leftrightarrow}"
             , decl "21C4" "\\ensuremath{\\rightleftarrows}"
             , decl "21D2" "\\ensuremath{\\Rightarrow}"
             , decl "2208" "\\ensuremath{\\in}"
             , decl "2227" "\\ensuremath{\\land}"
             , decl "25CF" "\\ensuremath{\\bullet}"
             , decl "2610" "\\ensuremath{\\square}"
             , decl "2713" "\\checkmark"
             , raw "\\else"
             , raw "  % XeTeX/LuaTeX path: map via newunicodechar"
             , raw "  \\usepackage{amsmath}"           -- for \\ensuremath
             , raw "  \\usepackage{newunicodechar}"
             , raw "  \\newcommand{\\textsub}[1]{\\ensuremath{_{#1}}}"
             , neu "2080" "\\textsub{0}"
             , neu "2081" "\\textsub{1}"
             , neu "2082" "\\textsub{2}"
             , neu "2083" "\\textsub{3}"
             , neu "2084" "\\textsub{4}"
             , neu "2085" "\\textsub{5}"
             , neu "2086" "\\textsub{6}"
             , neu "2087" "\\textsub{7}"
             , neu "2088" "\\textsub{8}"
             , neu "2089" "\\textsub{9}"
             , neu "2154" "\\ensuremath{\\tfrac{2}{3}}"
             , neu "2194" "\\ensuremath{\\leftrightarrow}"
             , neu "21c4" "\\ensuremath{\\rightleftarrows}"
             , neu "21d2" "\\ensuremath{\\Rightarrow}"
             , neu "2208" "\\ensuremath{\\in}"
             , neu "2227" "\\ensuremath{\\land}"
             , neu "25cf" "\\ensuremath{\\bullet}"
             , neu "2610" "\\ensuremath{\\square}"
             , neu "2713" "\\checkmark"
             , raw "\\fi"
             ]
      merged = case P.lookupMeta "header-includes" meta of
                 Just (P.MetaList xs) -> P.MetaList (xs <> incs)
                 Just mv -> P.MetaList $ mv : incs
                 Nothing -> P.MetaList incs
  in P.Pandoc (Pb.setMeta "header-includes" merged meta) blocks
  where
    raw :: String -> MetaValue
    raw s = P.MetaBlocks [P.RawBlock (P.Format "latex") (T.pack s)]

    -- pdfTeX mapping: \DeclareUnicodeCharacter{XXXX}{...}
    decl :: String -> String -> MetaValue
    decl hex body =
      raw $ "\\DeclareUnicodeCharacter{" <> hex <> "}{" <> body <> "}"

    -- Xe/Lua mapping: \newunicodechar{^^^^XXXX}{...}
    neu :: String -> String -> MetaValue
    neu hex body =
      raw $ "\\newunicodechar{^^^^" <> hex <> "}{" <> body <> "}"

neutralizeProblemMath :: Pandoc -> Pandoc
neutralizeProblemMath = walk fix
  where
    fix :: Inline -> Inline
    fix (Math _ s)
      | looksRegexLike s = Code nullAttr s
      | otherwise        = Math InlineMath s
    fix x = x

    looksRegexLike :: T.Text -> Bool
    looksRegexLike s =
         T.isInfixOf "\\d{" s
      || T.isInfixOf "\\("  s
      || T.isInfixOf "\\)"  s
      || T.isInfixOf "GF-NOTE-" s
      || T.isInfixOf "DMCC-GF-" s
      || T.isPrefixOf "^" s     -- typical regex anchor