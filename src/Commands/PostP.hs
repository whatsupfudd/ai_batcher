{-# LANGUAGE OverloadedStrings #-}

-- postp — HTML post-processor (v0.6)
-- Fixes:
--  • Correct span length: do not double-count the kind prefix when slicing the
--    original text; this previously caused extra trailing characters to be
--    wrapped (e.g., "DOC‑05 sch").
--  • Deep traversal within <p>/<li>, preserving already-decorated spans.
--  • Unicode hyphens normalization for matching only; original text preserved.
--  • Earliest-prefix selection prefers longer prefix when tie (SEC‑MKT‑ over SEC‑).

module Commands.PostP (doPostP) where

import           Control.Monad              (unless)
import           Data.Char                  (isDigit)
import           Data.Maybe                 (fromMaybe)
import           Data.Text                  (Text)
import qualified Data.Text                 as T
import qualified Data.Text.IO              as TIO
import           Data.Time                  (getCurrentTime)
import           System.Directory           (createDirectoryIfMissing, doesFileExist)
import           System.Environment         (getArgs)
import           System.Exit                (exitFailure)
import           System.FilePath            (takeDirectory, takeFileName, (</>))
import           Text.HTML.TagSoup          ( Tag(..)
                                            , Attribute
                                            , parseTags
                                            , renderTags )

import qualified Options.Runtime as Opt


--------------------------------------------------------------------------------
-- Types
--------------------------------------------------------------------------------

data DlvKind
  = GOV | SEC | OFF | TAX | TOK | PAY | VND | POL | DOC | OPN | INT | SEC_MKT | PLY
  deriving (Eq, Show)

-- Deliverable reference structure (rRaw preserves original substring)

data DlvRef = DlvRef
  { rKind  :: DlvKind
  , rMain  :: Text
  , rRange :: Maybe Text
  , rAlt   :: Maybe Text
  , rRaw   :: Text
  } deriving (Show)

--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

doPostP :: FilePath -> Opt.RunOptions -> IO ()
doPostP inPath rtOpts = do
  ok <- doesFileExist inPath
  unless ok $ putStrLn ("[FATAL] File not found: " ++ inPath) >> exitFailure

  html <- TIO.readFile inPath
  _ts  <- getCurrentTime

  let tags0     = parseTags html :: [Tag Text]
      assets    = mkHeadAssets
      tags1     = insertHeadAssets assets tags0
      tags2     = annotateDeliverableRefs tags1
      tagsFinal = fmap addTWClasses tags2
      outText   = renderTags tagsFinal

      outDir    = takeDirectory inPath </> "New"
      outPath   = outDir </> takeFileName inPath

  createDirectoryIfMissing True outDir
  TIO.writeFile outPath outText
  putStrLn $ "Annotated HTML written to: " ++ outPath

--------------------------------------------------------------------------------
-- Head assets (Tailwind + CSS)
--------------------------------------------------------------------------------

mkHeadAssets :: [Tag Text]
mkHeadAssets =
  [ TagOpen "script" [("src", "https://cdn.tailwindcss.com")]
  , TagClose "script"
  , TagOpen "link"   [("rel","stylesheet"), ("href","draftDlv_1.css")]
  , TagClose "link"
  ]

insertHeadAssets :: [Tag Text] -> [Tag Text] -> [Tag Text]
insertHeadAssets assets = go False
  where
    go _ [] = []
    go ins (t@(TagOpen name _) : rest)
      | not ins && T.toCaseFold name == "head" = t : assets ++ go True rest
      | otherwise                               = t : go ins rest
    go ins (t:rest) = t : go ins rest

--------------------------------------------------------------------------------
-- Tailwind class augmentation
--------------------------------------------------------------------------------

addTWClasses :: Tag Text -> Tag Text
addTWClasses (TagOpen name attrs)
  | n == "ol"         = TagOpen name (mergeClass attrs "list-decimal list-outside ml-8 my-6 space-y-1")
  | n == "ul"         = TagOpen name (mergeClass attrs "list-disc list-outside ml-8 my-6 space-y-1")
  | n == "li"         = TagOpen name (mergeClass attrs "my-1")
  | n == "p"          = TagOpen name (mergeClass attrs "my-3 text-justify")
  | n == "h1"         = TagOpen name (mergeClass attrs "text-2xl font-bold leading-tight mt-8 mb-3")
  | n == "h2"         = TagOpen name (mergeClass attrs "text-xl font-bold leading-tight mt-8 mb-3")
  | n == "h3"         = TagOpen name (mergeClass attrs "text-lg font-semibold leading-snug mt-6 mb-2")
  | n == "blockquote" = TagOpen name (mergeClass attrs "border-l-4 pl-4 text-gray-600 italic my-4")
  | n == "table"      = TagOpen name (mergeClass attrs "w-full border-collapse my-4")
  | n == "th"         = TagOpen name (mergeClass attrs "border px-2 py-1 font-semibold bg-gray-50 align-top")
  | n == "td"         = TagOpen name (mergeClass attrs "border px-2 py-1 align-top")
  | n == "hr"         = TagOpen name (mergeClass attrs "my-8 border-t")
  | otherwise         = TagOpen name attrs
  where n = T.toCaseFold name
addTWClasses t = t

mergeClass :: [Attribute Text] -> Text -> [Attribute Text]
mergeClass attrs new = upsertAttr "class" merged attrs
  where
    old    = fromMaybe "" (lookup "class" attrs)
    toks   = uniq (ws old ++ ws new)
    merged = T.intercalate " " toks
    ws   = filter (not . T.null) . T.words
    uniq = uniqBy []
    uniqBy seen [] = reverse seen
    uniqBy seen (x:xs)
      | any ((== T.toCaseFold x) . T.toCaseFold) seen = uniqBy seen xs
      | otherwise                                     = uniqBy (x:seen) xs

upsertAttr :: Eq a => a -> b -> [(a,b)] -> [(a,b)]
upsertAttr k v [] = [(k,v)]
upsertAttr k v ((k',v'):xs)
  | k == k'   = (k,v) : xs
  | otherwise = (k',v') : upsertAttr k v xs

--------------------------------------------------------------------------------
-- Deliverable scanning (precise & deep)
--------------------------------------------------------------------------------

-- Normalize hyphen-like characters to '-' for matching; original text preserved.
normalizeHyphens :: Text -> Text
normalizeHyphens = T.map rep
  where
    rep c | c == '-'  = '-'
          | c == '‐'  = '-'  -- U+2010 hyphen
          | c == '‑'  = '-'  -- U+2011 non-breaking hyphen
          | c == '‒'  = '-'  -- U+2012 figure dash
          | c == '–'  = '-'  -- U+2013 en dash
          | c == '—'  = '-'  -- U+2014 em dash
          | otherwise = c

-- Entry: annotate inside <p> and <li> (deep walk)
annotateDeliverableRefs :: [Tag Text] -> [Tag Text]
annotateDeliverableRefs = go
  where
    go [] = []
    go (TagOpen name attrs : xs) | isTarget name =
      let (inside, rest) = breakAtCloseDeep name xs
      in TagOpen name attrs : transformInline inside ++ TagClose name : go rest
    go (t:xs) = t : go xs

    isTarget n = let nc = T.toCaseFold n in nc == "p" || nc == "li"

-- Depth-aware slice until matching close of same name
breakAtCloseDeep :: Text -> [Tag Text] -> ([Tag Text], [Tag Text])
breakAtCloseDeep name = go (1 :: Int) []
  where
    eq a b = T.toCaseFold a == T.toCaseFold b
    go _ acc [] = (reverse acc, [])
    go d acc (t:ts) = case t of
      TagOpen n _ | eq n name -> go (d+1) (t:acc) ts
      TagClose n  | eq n name -> if d == 1 then (reverse acc, ts) else go (d-1) (t:acc) ts
      _                       -> go d (t:acc) ts

-- Deep transform within the captured body: recurse into nested inline tags and
-- decorate TagText segments. Skip already-decorated spans.
transformInline :: [Tag Text] -> [Tag Text]
transformInline = go
  where
    go [] = []
    go (TagText t : xs) = splitWithRefs t ++ go xs
    go (TagOpen n a : xs)
      | isDlvSpan n a =
          let (inside, rest) = breakAtCloseDeep n xs
          in TagOpen n a : inside ++ TagClose n : go rest
      | otherwise =
          let (inside, rest) = breakAtCloseDeep n xs
          in TagOpen n a : transformInline inside ++ TagClose n : go rest
    go (t:xs) = t : go xs

    isDlvSpan n attrs = T.toCaseFold n == "span" && hasClass attrs "dlv-ref"

hasClass :: [Attribute Text] -> Text -> Bool
hasClass attrs cls = maybe False match (lookup "class" attrs)
  where match v = any (== T.toCaseFold cls) (map T.toCaseFold (T.words v))

-- Streaming splitter that matches on a normalized (hyphen-folded) view while
-- slicing spans from the original text. **BUGFIX**: use exactly the number of
-- normalized characters consumed by the parser when slicing the original; do
-- NOT add the prefix length again.
splitWithRefs :: Text -> [Tag Text]
splitWithRefs t0 = go T.empty t0 (normalizeHyphens t0)
  where
    go acc orig norm
      | T.null norm = emit acc <> emit orig
      | otherwise =
          case earliestPrefix norm kindPrefixes of
            Nothing -> emit (acc <> orig)
            Just (preN, (pref, kind), sufN) ->
              let preLen  = T.length preN
                  preO    = T.take preLen orig
                  sufO    = T.drop preLen orig
              in case parseAfterPrefix kind pref sufN of
                   Just (refBase, restN) ->
                     let consumedN = T.length sufN - T.length restN  -- includes prefix
                         rawO      = T.take consumedN sufO           -- BUGFIX: no +length pref
                         restO     = T.drop consumedN sufO
                         ref       = refBase { rRaw = rawO }
                     in emit (acc <> preO) ++ spanForRef ref ++ go T.empty restO restN
                   Nothing ->
                     -- consume one original char and continue scanning
                     case T.uncons sufO of
                       Just (c, restO1) -> go (acc <> preO <> T.singleton c) restO1 (T.drop (preLen+1) (preN <> sufN))
                       Nothing          -> emit (acc <> preO)

    emit x | T.null x  = []
           | otherwise = [TagText x]

--------------------------------------------------------------------------------
-- Matching utilities
--------------------------------------------------------------------------------

-- Canonical code to emit in data-kind
kindCode :: DlvKind -> Text
kindCode k = case k of
  GOV     -> "GOV"; SEC -> "SEC"; OFF -> "OFF"; TAX -> "TAX"; TOK -> "TOK"; PAY -> "PAY"
  VND     -> "VND"; POL -> "POL"; DOC -> "DOC"; OPN -> "OPN"; INT -> "INT"
  SEC_MKT -> "SEC-MKT"; PLY -> "PLY"

-- Recognized prefixes (ASCII hyphens; normalization maps Unicode hyphens → '-')
kindPrefixes :: [(Text, DlvKind)]
kindPrefixes =
  [ ("SEC-MKT-", SEC_MKT)
  , ("GOV-", GOV), ("SEC-", SEC), ("OFF-", OFF), ("TAX-", TAX), ("TOK-", TOK)
  , ("PAY-", PAY), ("VND-", VND), ("POL-", POL), ("DOC-", DOC), ("OPN-", OPN)
  , ("INT-", INT), ("PLY-", PLY)
  ]

-- Earliest (left-most) prefix; prefer longer prefix when tie at same position
earliestPrefix :: Text -> [(Text, DlvKind)] -> Maybe (Text, (Text, DlvKind), Text)
earliestPrefix s = foldl step Nothing
  where
    step acc (p, k) =
      case T.breakOn p s of
        (pre, suf) | T.null suf -> acc
                   | otherwise  ->
                       let cand = Just (pre, (p,k), suf)
                       in case acc of
                            Nothing -> cand
                            Just (pre', (p',_), _) ->
                              if T.length pre < T.length pre'
                                then cand
                                else if T.length pre == T.length pre' && T.length p > T.length p'
                                  then cand
                                  else acc

-- Parse after a known prefix (works on normalized text). Returns (ref, restNorm).
parseAfterPrefix :: DlvKind -> Text -> Text -> Maybe (DlvRef, Text)
parseAfterPrefix k pref t0 = do
  t1 <- T.stripPrefix pref t0
  (d1, t2) <- takeNDigits 2 t1
  let (mRange, t3) = case T.stripPrefix ".." t2 of
                       Just s  -> case takeNDigits 2 s of
                                    Just (d2, rest) -> (Just d2, rest)
                                    Nothing         -> (Nothing, t2)
                       Nothing -> (Nothing, t2)
  let (mAlt,  t4) = case T.uncons t3 of
                       Just ('/', s) -> case takeNDigits 2 s of
                                          Just (d3, rest) -> (Just d3, rest)
                                          Nothing         -> (Nothing, t3)
                       _             -> (Nothing, t3)
  let rawNorm = kindCode k <> "-" <> d1
              <> maybe "" (\x -> ".." <> x) mRange
              <> maybe "" (\x -> "/"  <> x) mAlt
  pure ( DlvRef { rKind = k, rMain = d1, rRange = mRange, rAlt = mAlt, rRaw = rawNorm }
       , t4 )

-- Take exactly n digits; return (digits, rest)
takeNDigits :: Int -> Text -> Maybe (Text, Text)
takeNDigits n t = let (ds, rest) = T.splitAt n t
                  in if T.length ds == n && T.all isDigit ds then Just (ds, rest) else Nothing

--------------------------------------------------------------------------------
-- Emit span
--------------------------------------------------------------------------------

spanForRef :: DlvRef -> [Tag Text]
spanForRef r =
  [ TagOpen "span" ( base ++ opt "data-range-end" (rRange r) ++ opt "data-alt" (rAlt r) )
  , TagText (rRaw r)
  , TagClose "span"
  ]
  where
    base = [ ("class", "dlv-ref")
           , ("data-kind", kindCode (rKind r))
           , ("data-main", rMain r)
           ]
    opt _ Nothing  = []
    opt k (Just v) = [(k, v)]

