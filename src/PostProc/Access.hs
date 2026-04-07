module PostProc.Access where

import Data.Int (Int32)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Vector (Vector)
import Data.Time (UTCTime)

import qualified Data.Aeson as Ae

import Hasql.Pool (Pool, use)
import Hasql.Session (statement)

import qualified DB.PostProcStmt as Pp


fetchResultsForProduction :: Pool -> Text -> IO (Either String (Vector (Int32, UTCTime, Text)))
fetchResultsForProduction pgPool productionName = do
  rezA <- use pgPool $ statement productionName Pp.fetchResultsForProduction
  case rezA of
    Left err -> do
      putStrLn $ "@[fetchResultsForProduction] error: " <> show err
      pure . Left $ "@[fetchResultsForProduction] error: " <> show err
    Right results -> do
      pure . Right $ results
