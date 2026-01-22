{-# LANGUAGE QuasiQuotes #-}

module DB.TemplateStmt where

import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Vector (Vector)

import Data.Aeson (Value)

import Hasql.Statement (Statement)
import Hasql.TH
import Hasql.Pool (Pool, UsageError, use)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as TxS


insertS3Object :: Statement (UUID, Text, Int64) ()
insertS3Object =
  [resultlessStatement|
    insert into batcher.s3_objects (locator, kind, bytes)
    values ($1 :: uuid, $2 :: text, $3 :: int8)
    on conflict (locator) do nothing
  |]

insertTemplate :: Statement (Text, UUID) UUID
insertTemplate =
  [singletonStatement|
    insert into batcher.asset_templates (template_name, s3_locator)
    values ($1 :: text, $2 :: uuid)
    returning template_id :: uuid
  |]

insertSource :: Statement (Text, UUID) UUID
insertSource =
  [singletonStatement|
    insert into batcher.asset_sources (source_name, s3_locator)
    values ($1 :: text, $2 :: uuid)
    returning source_id :: uuid
  |]

insertProduction :: Statement (Text, UUID, UUID) UUID
insertProduction =
  [singletonStatement|
    insert into batcher.productions (production_name, template_id, source_id)
    values ($1 :: text, $2 :: uuid, $3 :: uuid)
    returning production_id :: uuid
  |]

insertRequest :: Statement (UUID, Int32, Value, Text) UUID
insertRequest =
  [singletonStatement|
    insert into batcher.requests
      (production_id, item_index, item_meta, request_text)
    values
      ($1 :: uuid, $2 :: int4, $3 :: jsonb, $4 :: text)
    returning request_id :: uuid
  |]

insertRequestEvent :: Statement (UUID, Text, Value) ()
insertRequestEvent =
  [resultlessStatement|
    insert into batcher.request_events
      (request_id, state, details)
    values
      ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]

