{-# LANGUAGE QuasiQuotes #-}

module DB.EngineStmt where

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

execStmt :: Pool -> Tx.Transaction a -> IO (Either UsageError a)
execStmt pool tx = use pool $ TxS.transaction TxS.ReadCommitted TxS.Write tx


-- Submit Stmts:
-- Only updates if claim token matches (prevents stale worker updates).
markSubmittedStmt :: Statement (UUID, UUID, Text, Maybe Text) ()
markSubmittedStmt =
  [resultlessStatement|
    update batcher.requests set
      state = 'submitted',
      provider_batch_id = $3 :: text,
      provider_request_id = $4 :: text?,
      submit_claimed_until = null,
      submit_claimed_by = null,
      submit_claim_token = null,
      updated_at = now()
     where request_id = $1 :: uuid
       and submit_claim_token = $2 :: uuid
  |]


releaseClaimStmt :: Statement (UUID, UUID) ()
releaseClaimStmt =
  [resultlessStatement|
    update batcher.requests set 
      submit_claimed_until = null,
      submit_claimed_by = null,
      submit_claim_token = null,
      updated_at = now()
     where request_id = $1 :: uuid
       and submit_claim_token = $2 :: uuid
  |]


insertRequestEventStmt :: Statement (UUID, Text, Value) ()
insertRequestEventStmt =
  [resultlessStatement|
    insert into batcher.request_events
      (request_id, state, details)
    values
      ($1::uuid, $2::text::batcher.request_state, $3::jsonb)
  |]


-- Returns (request_id, request_text)
claimRequestsStmt :: Statement (Int32, Text, UUID, Int32) (Vector (UUID, Text))
claimRequestsStmt =
  [vectorStatement|
    with picked as (
      select r.request_id, r.request_text
      from batcher.requests r
      where r.state = 'entered'
        and (r.submit_claimed_until is null or r.submit_claimed_until < now())
      order by r.created_at asc
      limit $1 :: int4
      for update skip locked
    )
    update batcher.requests u
      set submit_claimed_by    = $2 :: text,
          submit_claim_token   = $3 :: uuid,
          submit_claimed_until = now() + make_interval(secs => $4 :: int4),
          updated_at           = now()
    from picked
    where u.request_id = picked.request_id
    returning u.request_id :: uuid, picked.request_text :: text
  |]

-- Poll Stmts:

-- Returns one row per claimed batch UUID.
claimBatchesStmt :: Statement (Int32, Text, UUID, Int32) (Vector UUID)
claimBatchesStmt =
  [vectorStatement|
    with picked as (
      select distinct on (r.provider_batch_uuid)
             r.request_id,
             r.provider_batch_uuid
        from batcher.requests r
       where r.state = 'submitted'
         and r.provider_batch_uuid is not null
         and (r.poll_claimed_until is null or r.poll_claimed_until < now())
       order by r.provider_batch_uuid, r.updated_at asc
       limit $1 :: int4
       for update skip locked
    ),
    mark_one as (
      update batcher.requests r
         set poll_claimed_by    = $2 :: text,
             poll_claim_token   = $3 :: uuid,
             poll_claimed_until = now() + make_interval(secs => $4 :: int4),
             updated_at         = now()
        from picked p
       where r.request_id = p.request_id
       returning p.provider_batch_uuid
    ),
    mark_all as (
      update batcher.requests r
         set poll_claimed_by    = $2 :: text,
             poll_claim_token   = $3 :: uuid,
             poll_claimed_until = now() + make_interval(secs => $4 :: int4),
             updated_at         = now()
        from mark_one m
       where r.provider_batch_uuid = m.provider_batch_uuid
       returning 1
    )
    select provider_batch_uuid :: uuid
      from mark_one
  |]


markBatchCompletedStmt :: Statement (UUID, UUID) (Vector UUID)
markBatchCompletedStmt =
  [vectorStatement|
    update batcher.requests
       set state      = 'completed',
           updated_at = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'submitted'
       and poll_claim_token   = $2 :: uuid
     returning request_id :: uuid
  |]

markBatchCancelledStmt :: Statement (UUID, UUID) (Vector UUID)
markBatchCancelledStmt =
  [vectorStatement|
    update batcher.requests
       set state      = 'cancelled',
           updated_at = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'submitted'
       and poll_claim_token   = $2 :: uuid
     returning request_id :: uuid
  |]

clearPollClaimBatchStmt :: Statement (UUID, UUID) ()
clearPollClaimBatchStmt =
  [resultlessStatement|
    update batcher.requests
       set poll_claimed_until = null,
           poll_claimed_by    = null,
           poll_claim_token   = null,
           updated_at         = now()
     where provider_batch_uuid = $1 :: uuid
       and poll_claim_token    = $2 :: uuid
  |]


insertFetchOutboxStmt :: Statement UUID ()
insertFetchOutboxStmt =
  [resultlessStatement|
    insert into batcher.fetch_outbox (provider_batch_uuid)
    values ($1 :: uuid)
    on conflict (provider_batch_uuid) do nothing
  |]


-- Fetch Stmts:
claimFetchOutboxManyStmt :: Statement (Int32, Text, UUID, Int32) (Vector UUID)
claimFetchOutboxManyStmt =
  [vectorStatement|
    with picked as (
      select o.provider_batch_uuid
        from batcher.fetch_outbox o
       where (o.fetch_claimed_until is null or o.fetch_claimed_until < now())
       order by o.created_at asc
       limit $1 :: int4
       for update skip locked
    )
    update batcher.fetch_outbox o
       set fetch_claimed_by    = $2 :: text,
           fetch_claim_token   = $3 :: uuid,
           fetch_claimed_until = now() + make_interval(secs => $4 :: int4)
      from picked p
     where o.provider_batch_uuid = p.provider_batch_uuid
     returning o.provider_batch_uuid :: uuid
  |]

claimFetchOutboxOneStmt :: Statement (UUID, Text, UUID, Int32) (Vector UUID)
claimFetchOutboxOneStmt =
  [vectorStatement|
    update batcher.fetch_outbox
       set fetch_claimed_by    = $2 :: text,
           fetch_claim_token   = $3 :: uuid,
           fetch_claimed_until = now() + make_interval(secs => $4 :: int4)
     where provider_batch_uuid = $1 :: uuid
       and (fetch_claimed_until is null or fetch_claimed_until < now())
     returning provider_batch_uuid :: uuid
  |]

-- Attach raw_result_locator to requests in that batch (only those completed).
-- We do "set if null" so repeat fetches are harmless.
attachRawLocatorStmt :: Statement (UUID, UUID) (Vector UUID)
attachRawLocatorStmt = [vectorStatement|
    update batcher.requests
       set raw_result_locator = coalesce(raw_result_locator, $2 :: uuid),
           updated_at         = now()
     where provider_batch_uuid = $1 :: uuid
       and state              = 'completed'
     returning request_id :: uuid
  |]


insertS3ObjectStmt :: Statement (UUID, Text, Int64, Text) ()
insertS3ObjectStmt = [resultlessStatement|
    insert into batcher.s3_objects (locator, kind, bytes, content_type)
    values ($1 :: uuid, $2 :: text, $3 :: int8, $4 :: text)
    on conflict (locator) do nothing
  |]


deleteFetchOutboxStmt :: Statement (UUID, UUID) ()
deleteFetchOutboxStmt = [resultlessStatement|
    delete from batcher.fetch_outbox
     where provider_batch_uuid = $1 :: uuid
       and fetch_claim_token   = $2 :: uuid
  |]


releaseFetchOutboxClaimStmt :: Statement (UUID, UUID, Int32) ()
releaseFetchOutboxClaimStmt = [resultlessStatement|
    update batcher.fetch_outbox
       set fetch_claimed_by    = null,
           fetch_claim_token   = null,
           fetch_claimed_until = now() + make_interval(secs => $3 :: int4)
     where provider_batch_uuid = $1 :: uuid
       and fetch_claim_token   = $2 :: uuid
  |]

listRequestsForBatchStmt :: Statement UUID (Vector UUID)
listRequestsForBatchStmt = [vectorStatement|
    select request_id :: uuid
      from batcher.requests
     where provider_batch_uuid = $1 :: uuid
       and state = 'completed'
  |]
