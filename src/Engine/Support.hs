module Engine.Support where

import Control.Concurrent.STM
import Control.Concurrent.STM.TBQueue


tryWriteTBQueue :: TBQueue a -> a -> STM Bool
tryWriteTBQueue q x = do
  full <- isFullTBQueue q
  if full
    then pure False
    else writeTBQueue q x >> pure True
