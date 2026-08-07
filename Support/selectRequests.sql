select
    request_id
  , production_id
  , item_index
  , item_meta
  , left(request_text, 10) || '...'
  , state
  , provider_batch_id
  , provider_request_id
  , raw_result_locator
  , final_result_locator
  , submit_claimed_until
  , submit_claimed_by
  , submit_claim_token
  , provider_batch_uuid
  , poll_claimed_until
  , poll_claimed_by
  , poll_claim_token
  , created_at
  , updated_at
from requests
order by created_at desc limit 1;

