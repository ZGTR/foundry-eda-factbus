# Shared Events (factbus) IaC modules

Three OpenTofu/Terraform modules, one per role. None of them declares a provider: the
root module that calls them sets region, credentials and, locally, the LocalStack
endpoints. The same module source therefore serves LocalStack and a real account.

| Module | Who instantiates it | What it creates |
| --- | --- | --- |
| `modules/bus` | whoever owns the bus, once per environment | the EventBridge bus and a 30-day archive for replay (capped below the 35-day inbox retention) |
| `modules/producer` | each producing service | an IAM policy allowing `PutEvents` under the service's own `source` only, plus the 1-minute sweeper schedule |
| `modules/consumer` | each consuming service | the rule, its target retry policy and delivery DLQ, the queue and its processing DLQ, event source mapping (with `ReportBatchItemFailures`), four alarms, the DynamoDB inbox table and the function's IAM policy; optional `tags` on every taggable one |

## Smallest calls

```hcl
module "bus" {
  source = "github.com/ZGTR/foundry-eda-factbus//iac/modules/bus"
  name   = "events"
}

module "orders_producer" {
  source               = "github.com/ZGTR/foundry-eda-factbus//iac/modules/producer"
  bus_arn              = module.bus.bus_arn
  event_source         = "shop.orders"
  role_name            = aws_iam_role.orders.name
  sweeper_function_arn = aws_lambda_function.orders_sweeper.arn
}

module "ledger" {
  source          = "github.com/ZGTR/foundry-eda-factbus//iac/modules/consumer"
  name            = "ledger"
  bus_name        = module.bus.bus_name
  sources         = ["shop.orders"]
  event_types     = ["order.placed"]
  function_arn    = aws_lambda_function.ledger.arn
  role_name       = aws_iam_role.ledger.name
  alarm_topic_arn = aws_sns_topic.oncall.arn
}
```

Pass the consumer function `FACTBUS_INBOX_TABLE = module.ledger.inbox_table_name` and give
that value to `build_dynamo_inbox(table_name=...)`. That single table holds the inbox, the
applied versions and `once` claims.

To narrow a consumer to part of a type's traffic, set `detail_pattern`. It is merged
into the rule beside `source` and `detail-type` as the pattern's `detail`, in EventBridge
pattern syntax:

```hcl
module "whatsapp_sender" {
  # ... as above ...
  event_types    = ["message.requested"]
  detail_pattern = { channel = ["whatsapp"] }   # an event with channel = "email" never reaches this queue
}
```

Null, the default, leaves the rule on `source` and `detail-type` only.

A consumer that must hear several buses on one queue (one kill switch, one place that
orders its own updates) sets `subscriptions`. Each entry becomes a rule named
`<name>-<key>` on its own bus, with its own EventBridge pattern written as JSON, and
every rule targets the same queue with the same retry policy and delivery DLQ. The
queue's one policy, and the delivery DLQ's, admit exactly those rules:

```hcl
module "results" {
  # ... name, function_arn, role_name, alarm_topic_arn as above; no bus_name, sources or event_types ...
  subscriptions = [
    { key = "results", bus_name = module.comms_bus.bus_name,
      event_pattern = jsonencode({ source = [{ "anything-but" = ["comms-core"] }], "detail-type" = ["comms.delivery.accepted"] }) },
    { key = "answers", bus_name = var.product_bus_name,
      event_pattern = jsonencode({ source = ["shop.backend"], "detail-type" = ["product.action.completed"] }) },
  ]
}
```

Keys are stable names: adding or removing an entry leaves the other rules alone. Setting
both forms, or neither, is refused at plan. A stack applied before `subscriptions`
existed keeps its single rule and target (the module carries `moved` blocks to their new
`[0]` addresses).

`tags` (a map of strings, default `{}`) is set on every taggable resource the consumer
creates: the rule or each subscription rule, the queue, its DLQ, the delivery DLQ, the
four alarms and the inbox table. The event source mapping, rule targets, queue policies
and the inline role policy carry none. A product whose own gates read a queue tag sets it
here, for example Shepherd's notifications queues:

```hcl
module "core_results" {
  # ... as above ...
  tags = { "comms:backoff-waits-seconds" = "[30,120,600,1800,3600,7200,7200,7200,7200]" }
}
```

The default adds no tag, so an existing stack's plan is unchanged. A provider's
`default_tags` still apply beside these.

## Local and production

| | Local | Production |
| --- | --- | --- |
| Provider | `endpoints { events = sqs = dynamodb = lambda = iam = sts = cloudwatch = logs = "http://localhost:4576" }`, `skip_credentials_validation`, `skip_requesting_account_id`, `access_key = secret_key = "test"` | no `endpoints` block; region and the deploy role select the account |
| Function env | nothing extra: LocalStack sets `AWS_ENDPOINT_URL` in its Lambda containers; the handler passes it to factbus as `endpoint_url` (factbus reads no environment) | unset |
| Proof | `make e2e` (see `examples/hello`) | the same example applied to a sandbox account |

## Known limits

- `role_name` and `sweeper_function_arn` must be known at plan time, because they
  decide `count`. A role created with `name_prefix` in the same apply is not, so attach
  `consumer_policy_json` / `policy_json` yourself in that case.
- Queue-first deploys: `create_event_source_mapping = false` creates the rule, queue,
  DLQs, their alarms and the inbox, and outputs `queue_arn`, with no event source mapping
  and no Lambda Errors alarm, so `function_arn` may be empty. Apply again with `true` once
  the function exists. `enabled` stays the kill switch on an existing mapping.
- The mapping is uncapped unless `maximum_concurrency` is set (2-1000). A function with
  reserved concurrency should set it at or below that reservation: otherwise the poller
  invokes past it, throttled batches return to the queue, and each return counts toward
  `max_receive_count`.
- One queue, several buses: set `subscriptions` instead of `bus_name`, `sources` and
  `event_types` (see below). Two queues for two buses is still two module calls.
- A consumer's two DLQs mean different things. `<name>-delivery-dlq` holds events
  EventBridge could not put on the queue (queue policy, deleted or KMS-locked queue)
  after `delivery_max_retry_attempts` / `delivery_max_event_age_seconds` (defaults: the
  EventBridge maximum, 185 attempts over 24 h). `<name>-dlq` holds messages the handler
  failed `max_receive_count` times. Redrive the first back onto the queue once the
  delivery fault is fixed; the inbox makes a redriven copy harmless.
- Alarms notify nobody when `alarm_topic_arn` is null. They still exist and show state in
  the console.

## Tests

```bash
cd iac/modules/consumer && tofu init -backend=false && tofu test
```
