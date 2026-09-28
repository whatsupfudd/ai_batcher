module Service.OpenAI.Cache where

import Data.Text (Text)

import Data.Aeson as Ae
import Data.Aeson.Types as AeT

import Service.OpenAI.Types
import Utils as Ut


data CacheKey = CacheKey {
  keyCK :: Text
  , policyCK :: CachePolicy
  } deriving (Show)


calcKey :: ServiceConfig -> [Text] -> CacheKey
calcKey cfg ctxts =
  let
    fullText = foldl (\acc t -> acc <> t) "" ctxts
    key = "doc-fnv1a64-" <> (Ut.toHex64Text $ Ut.fnv1a64Text fullText)
  in
  CacheKey key cfg.cachePolicySC


jsonFields :: CachePolicy -> CacheKey -> [AeT.Pair]
jsonFields = undefined


inputValue :: CachePolicy -> Text -> Text -> Ae.Value
inputValue = undefined

buildCacheInfo :: CachePolicy -> Text -> [AeT.Pair]
buildCacheInfo policy key = 
  case policy.generationCP of
    NoneCG -> []
    SimpleCG -> [
      "prompt_cache_key" .= key
      , "prompt_cache_retention" .= showRetention policy.retentionCP
      ]
    Inter_55CG -> [
      "prompt_cache_key" .= key
      , "prompt_cache_retention" .= ("24h" :: Text)
      ]
    Block_56CG -> [
      "prompt_cache_key" .= key
      , "prompt_cache_options" .= Ae.object [
        "mode" .= ("explicit" :: Text)
        , "ttl" .= ("30m" :: Text)
        ]
      ]


promptCacheInfo :: CachePolicy -> [AeT.Pair]
promptCacheInfo policy =
  case policy.generationCP of
    Block_56CG -> [ ("prompt_cache_breakpoint", Ae.object [ "mode" .= ("explicit" :: Text) ]) ]
    _ -> []
  