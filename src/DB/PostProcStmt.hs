
{-# LANGUAGE QuasiQuotes #-}

module DB.PostProcStmt where

import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Vector (Vector)
import Data.Time (UTCTime)

import Data.Aeson (Value)

import Hasql.Statement (Statement)
import Hasql.TH
import Hasql.Pool (Pool, UsageError, use)


fetchResultsForProduction :: Statement Text (Vector (Int32, UTCTime, Text))
fetchResultsForProduction = [vectorStatement|
  select
    b.item_index::int4
    , a.created_at::timestamptz
    , a.content::text
  from batcher.request_results a
    join batcher.requests b on a.request_fk = b.request_id
    join batcher.productions c on b.production_id = c.production_id
  where c.production_name = $1::text
  order by b.item_index
  |]
