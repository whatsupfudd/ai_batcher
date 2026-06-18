module PostProc.Convert where

import Control.Monad (when, void)
import qualified Data.ByteString.Lazy as Bsl
import Data.Char (chr)
import Data.Foldable (asum)
import Numeric (readHex)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Text.Regex.TDFA ((=~))
import Data.Void (Void)

import System.FilePath ( (</>), (<.>), takeExtension, splitFileName )
import System.Directory (createDirectoryIfMissing)

import Text.Megaparsec
import Text.Megaparsec.Char

import Text.Pandoc
import Text.Pandoc.Class (runIO)

type Parser = Parsec Void Text

-- | Unescape common escaped characters like \n, \t, \\, and Unicode (\uXXXX) sequences in a Text.
unescapeText :: T.Text -> T.Text
unescapeText = T.pack . unescWord . T.unpack
  where
    unescWord [] = []
    unescWord ('\\':'n':xs)  = '\n' : unescWord xs
    unescWord ('\\':'t':xs)  = '\t' : unescWord xs
    unescWord ('\\':'r':xs)  = '\r' : unescWord xs
    unescWord ('\\':'\\':xs) = '\\' : unescWord xs
    unescWord ('\\':'u':a:b:c:d:xs)
      | all isHex [a,b,c,d] =
          let hex = [a,b,c,d]
              val = chr $ fst $ head $ readHex hex
          in val : unescWord xs
    unescWord (x:xs) = x : unescWord xs

    isHex c = c `elem` (['0'..'9'] ++ ['a'..'f'] ++ ['A'..'F'])


convToDocx :: FilePath -> String -> Text -> IO ()
convToDocx outputDir outName content =
  -- Read the markdown content from the input file
  let
    markdownContent = unescapeText content
    outputPath = outputDir </> outName <.> "docx"
  in do
    -- Run the Pandoc conversion in the 'IO' monad
    result <- runIO $ do
        -- Read the markdown content into a Pandoc document (AST)
        pandocDoc <- readMarkdown def markdownContent
        -- Write the Pandoc document to a Docx formatted Text
        writeDocx def pandocDoc

    -- Handle the result of the conversion
    case result of
        Left err -> putStrLn $ "Error during conversion: " <> show err
        Right docxContent -> do
          -- Pandoc's writeDocx function outputs binary data (ByteString)
          Bsl.writeFile outputPath docxContent
          putStrLn $ "Successfully converted to " <> outputPath <> ". (Note: Actual file writing requires ByteString handling.)"


data CodeDef = CodeDef {
  content :: Text
  , lang :: String
  , fileName :: String
  }

convToHtmlJS :: FilePath -> Text -> IO ()
convToHtmlJS outputDir content =
  let
    markdownContent = unescapeText content
    (eiHtmlCode, eiJsCode) = extractWebPage markdownContent
  in do
  putStrLn $ "@[convToCode] content: " <> T.unpack (T.take 60 content)
  case (eiHtmlCode, eiJsCode) of
    (Right htmlCode, Right jsCode) -> do
      TIO.writeFile (outputDir </> saneFileName htmlCode.fileName) htmlCode.content
      TIO.writeFile (outputDir </> saneFileName jsCode.fileName) jsCode.content
    _ ->
      let
        errMsg = unlines [either (<> "Error in HTML code: ") (const "") eiHtmlCode, either (<> "Error in JS code. ") (const "") eiJsCode]
      in
      putStrLn errMsg


convToCode :: FilePath -> Text -> IO ()
convToCode outputDir content =
  let
    markdownContent = unescapeText content
    eiCode = extractCode markdownContent
  in do
  putStrLn $ "@[convToCode] content: " <> T.unpack (T.take 60 content)
  case eiCode of
    Right code ->
      let
        (dirs, fname) = splitFileName code.fileName
        destDir = outputDir </> dirs
      in do
      createDirectoryIfMissing True destDir
      TIO.writeFile (destDir </> fname) code.content
    Left errMsg ->
      let
        outMsg = "@[convToCode] err: " <> errMsg
      in
      putStrLn outMsg


saneFileName :: String -> String
saneFileName fname =
  T.unpack $ T.replace "'" " " $ T.replace "\"" " " $ T.replace "/" " " $ T.replace "\\" " " $
      T.replace ":" " " $ T.replace "*" " " $ T.replace "?" " " $ T.replace "<" " " $ T.replace ">" " " $
      T.replace "|" " " $ T.replace "\n" " " $ T.replace "\r" " " $ T.replace "\t" " " $ T.replace "\b" " " $
      T.replace "\f" " " $ T.replace "\v" " " $ T.pack fname
  
{-
The format of the input is:
`filename.html`
```html
<html>
<body>
<h1>Hello, world!</h1>
</body>
</html>
```
`filename.js`
```js
console.log("Hello, world!");
```
-}

extractWebPage :: Text -> (Either String CodeDef, Either String CodeDef)
extractWebPage content =
  case runParser webPageParser "" content of
    Left err -> (Left (errorBundlePretty err), Left (errorBundlePretty err))
    Right (htmlDef, jsDef) -> (Right htmlDef, Right jsDef)

webPageParser :: Parser (CodeDef, CodeDef)
webPageParser = do
  htmlDef <- codeDefBlock "html"
  jsDef <- codeDefBlock "js"
  pure (htmlDef, jsDef)


extractCode :: Text -> Either String CodeDef
extractCode content =
  case runParser anonCodeExtract "" content of
    Left err -> Left (errorBundlePretty err)
    Right codeDef -> Right codeDef


codeDefBlock :: String -> Parser CodeDef
codeDefBlock ext = do
  skipManyTill anySingle (lookAhead (fileNameLine ext))
  fname <- fileNameLine ext
  code <- codeBlock ext
  pure CodeDef
    { fileName = fname
    , lang = ext
    , content  = code
    }


{-
Anonymous code extract:
**File: `Syntax/Grammar.hs`**

```<language>
[... code ...]
-}
anonCodeExtract :: Parser CodeDef
anonCodeExtract = do
  -- many (char ' ' <|> char '\t')
  some (oneOf ("*#-" :: String))
  space
  optional $ string "File:"
  space
  hasQuote <- option False $ True <$ string "`"
  fname <- some (alphaNumChar <|> oneOf (".-_/" :: String))
  when hasQuote $ void $ char '`'
  many (oneOf ("*#-" :: String))
  void $ many eol
  many (char ' ' <|> char '\t')
  string "```"
  language <- some alphaNumChar
  void eol
  body <- manyTill anySingle (lookAhead closingFence)
  _ <- closingFence
  pure CodeDef { 
      fileName = fname
    , lang = language
    , content = dropFinalEol (T.pack body)
    }


closingFence :: Parser ()
closingFence = do
  string "```"
  void eol <|> eof


fileNameLine :: String -> Parser String
fileNameLine ext = try $ do
  many (char ' ' <|> char '\t')
  hasTick <- option Nothing $ Just <$> char '`'
  fname <- some (alphaNumChar <|> oneOf (".-_/" :: String))
  case hasTick of
    Just _ -> void $ char '`'
    Nothing -> pure ()
  if takeExtension fname == "." <> ext
    then pure ()
    else fail ("Not matching filename extension: " <> fname <> ", ext: " <> ext)
  many (char ' ' <|> char '\t')
  void eol <|> eof
  pure fname


codeBlock :: String -> Parser Text
codeBlock extension = try $ do
  many eol
  string "```"
  matchLanguage extension
  body <- manyTill anySingle (lookAhead closingFence)
  _ <- closingFence
  pure (dropFinalEol (T.pack body))


matchLanguage :: String -> Parser ()
matchLanguage extension = do
  case extension of
    "html" -> void (string "html")
    "js"   -> void (string "js" <|> string "javascript")
    _      -> fail "Invalid language"
  many (char ' ' <|> char '\t')
  void eol

dropFinalEol :: Text -> Text
dropFinalEol t =
  case T.stripSuffix "\r\n" t of
    Just t' -> t'
    Nothing ->
      case T.stripSuffix "\n" t of
        Just t' -> t'
        Nothing -> t

{-
-- | Top-level extractWebPage using Megaparsec
extractWebPage :: Text -> (Either String CodeDef, Either String CodeDef)
extractWebPage content =
  let parseRes = runParser webPageParser "" content
  in case parseRes of
    Left err -> (Left (errorBundlePretty err), Left (errorBundlePretty err))
    Right (htmlDef, jsDef) -> (Right htmlDef, Right jsDef)

-- Parse the input looking for both HTML and JS code blocks with filenames
webPageParser :: Parser (CodeDef, CodeDef)
webPageParser = do
  fileName <- skipManyTill anySingle (lookAhead $ fileNameLine "html")
  htmlDef <- codeDefBlock "html"
  skipManyTill anySingle (lookAhead $ fileNameLine "js")
  jsDef <- codeDefBlock "js"
  return (htmlDef, jsDef)

-- | Parse a file block (filename, code block) given expected ext
codeDefBlock :: String -> Parser CodeDef
codeDefBlock ext = do
  -- Accept a filename line anywhere before code block, not strictly after a newline
  skipManyTill anySingle (fileNameLine ext)
  fname <- fileNameLine ext
  optional space
  code <- codeBlock ext
  return $ CodeDef { content = code, fileName = fname }

-- Parse and return the filename (e.g. "something.html" or "something.js")
fileNameLine :: String -> Parser String
fileNameLine ext = try $ do
  -- allow it to appear after optional whitespace, optionally wrapped in backticks
  space
  char '`'
  fname <- some (alphaNumChar <|> oneOf (".-_" :: String))
  char '`'
  optional spaceChar
  if takeExtension fname == ("." <> ext) then
      pure fname
  else 
    fail ("Not matching filename extension: " <> fname <> ", ext: " <> ext)

-- Parse a markdown code block start with ```html or ```js and get the content
codeBlock :: String -> Parser Text
codeBlock extension = try $ do
  -- Skip until triple backticks with the expected lang
  manyTill anySingle (try (string "```"))
  matchLanguage extension 
  codeLines <- manyTill anySingle (try (eol *> string "```"))
  return $ T.strip (T.pack codeLines)


matchLanguage :: String -> Parser ()
matchLanguage extension = do
  optional spaceChar
  case extension of
    "html" -> do
      string "html"
    "js" -> asum [
        string "js"
        , string "javascript"
      ]
    _ -> do
      fail "Invalid language"
  eol
  return ()
-}