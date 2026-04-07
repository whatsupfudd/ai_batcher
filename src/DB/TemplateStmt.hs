{-# LANGUAGE QuasiQuotes #-}

module DB.TemplateStmt where

import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID (UUID)
import Data.Vector (Vector)

import Data.Aeson (Value)

import Hasql.Statement (Statement)
import Hasql.TH
import Hasql.Pool (Pool, UsageError, use)


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


fetchTemplate :: Statement Text (Maybe (UUID, UUID))
fetchTemplate =
  [maybeStatement|
    select
      template_id::uuid, s3_locator::uuid
    from batcher.asset_templates
    where template_name = $1::text
    order by created_at desc
    limit 1
  |]

fetchTemplateLocator :: Statement UUID UUID
fetchTemplateLocator =
  [singletonStatement|
    select s3_locator::uuid
    from batcher.asset_templates
    where template_id = $1::uuid
  |]


type TemplateInfo = (Text, UUID, UUID, UTCTime)

listAllTemplates :: Statement () (Vector TemplateInfo)
listAllTemplates =
  [vectorStatement|
    select
      template_name::text, template_id::uuid, s3_locator::uuid, created_at::timestamptz
    from batcher.asset_templates
    order by template_name, created_at desc
  |]

listTemplates :: Statement Text (Vector TemplateInfo)
listTemplates =
  [vectorStatement|
    select
      template_name::text, template_id::uuid, s3_locator::uuid, created_at::timestamptz
    from batcher.asset_templates
    where template_name ilike $1::text
    order by template_name, created_at desc
  |]

deleteTemplate :: Statement UUID ()
deleteTemplate =
  [resultlessStatement|
    delete from batcher.asset_templates
    where template_id = $1::uuid
  |]