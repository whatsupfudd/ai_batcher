module Utils (fnv1a64, toHex64) where

import qualified Data.ByteString as BS
import Data.Word ( Word64, Word8 )
import Data.Bits ( xor )
import Numeric ( showHex )


-- FNV-1a 64-bit (to derive a stable default prompt_cache_key)
fnv1a64 :: BS.ByteString -> Word64
fnv1a64 = BS.foldl' step offset
  where
    offset :: Word64
    offset = 14695981039346656037
    prime  :: Word64
    prime  = 1099511628211
    step :: Word64 -> Word8 -> Word64
    step h b = (h `xor` fromIntegral b) * prime

-- Hex printer for Word64
toHex64 :: Word64 -> String
toHex64 w = let s = showHex w ""
            in replicate (16 - length s) '0' ++ s
