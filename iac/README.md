# Shared Events (factbus) IaC modules

Three OpenTofu/Terraform modules, one per role. None of them declares a provider: the
root module that calls them sets region, credentials and, locally, the LocalStack
endpoints. The same module source therefore serves LocalStack and a real account.

| Module | Who instantiates it | What it creates |
| --- | --- | --- |
| `modules/bus` | whoever owns the bus, once per environment | the EventBridge bus and a 30-day archive for replay (capped below the 35-day inbox retention) |
| `modules/producer` | each producing service | an IAM policy allowing `PutEvents` under the service's own `source` only, plus the 1-minute sweeper schedule |
| `modules/consumer` | each consuming service | the rule, its target retry policy and delivery DLQ, the queue and its processing DLQ, event source mapping (with `ReportBatchItemFailures`), four alarms, the DynamoDB inbox table and the function's IAM policy |

## Smallest calls

```hcl
module "bus" {
  source = "github.com/ZGTR/mshaker-shared-events//iac/modules/bus"
  name   = "events"
}

module "orders_producer" {
  source               = "github.com/ZGTR/mshaker-shared-events//iac/modules/producer"
  bus_arn              = module.bus.bus_arn
  event_source         = "shop.orders"
  role_name            = aws_iam_role.orders.name
  sweeper_function_arn = aws_lambda_function.orders_sweeper.arn
}

module "ledger" {
  source          = "github.com/ZGTR/mshaker-shared-events//iac/modules/consumer"
  name            = "ledger"
  bus_name        = module.bus.bus_name
  sources         = ["shop.orders"]
  event_types     = ["order.placed"]
  function_arn    = aws_lambda_function.ledger.arn
  role_name       = aws_iam_role.ledger.name
  alarm_topic_arn = aws_sns_topic.oncall.arn
}
```

Pass the consumer function `FACTBUS_INBOX_TABLE = module.ledger.inbox_table_name`. That
single table holds the inbox, the applied versions and `once` claims.

## Local and production

| | Local | Production |
| --- | --- | --- |
| Provider | `endpoints { events = sqs = dynamodb = lambda = iam = sts = cloudwatch = logs = "http://localhost:4576" }`, `skip_credentials_validation`, `skip_requesting_account_id`, `access_key = secret_key = "test"` | no `endpoints` block; region and the deploy role select the account |
| Function env | nothing extra: LocalStack sets `AWS_ENDPOINT_URL` in its Lambda containers and factbus uses it (`FACTBUS_ENDPOINT_URL` overrides) | unset |
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
- One bus per consumer module. A consumer of two buses instantiates the module twice,
  once per bus, with distinct `name`s.
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
