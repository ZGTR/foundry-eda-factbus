# hello: one producer, one bus, two independent consumers

`hello.said` events go onto an EventBridge bus. Two `consumer` module instances,
`ledger` and `audit`, each route them to their own queue, and each runs the same
`handler.py` as its own Lambda (`<prefix>-ledger`, `<prefix>-audit`). Each Lambda writes
one item per event to its own ledger table, in the same DynamoDB transaction as its own
factbus inbox record. A second copy of the same CloudEvents `id` writes nothing. An
event with `data.poison = true` raises, is retried with backoff, and lands in each
consumer's processing DLQ after three receives. An event EventBridge cannot put on a
queue lands in that consumer's delivery DLQ instead.

Each consumer is one module block in `main.tf`, instantiated per consumer:

```hcl
module "consumer" {
  source             = "../../iac/modules/consumer"
  for_each           = local.consumers           # ledger, audit
  name               = "${var.prefix}-${each.key}"
  bus_name           = module.bus.bus_name
  sources            = [local.event_source]
  event_types        = [local.event_type]
  function_arn       = aws_lambda_function.hello[each.key].arn
  role_name          = aws_iam_role.hello[each.key].name
  max_receive_count  = 3
  create_inbox_table = true
  inbox_table_name   = "${var.prefix}-${each.key}-inbox"
}
```

It creates no VPC, subnet, NAT gateway or database instance. The inboxes and the
ledgers are DynamoDB tables. `prefix` (default `hello`) names every resource, so a
sandbox run does not collide with other stacks.

## Run it on LocalStack

From the repo root, with `tofu` and `uv` on PATH and Docker running:

```bash
uv venv -q -p 3.12 .venv && uv pip install -q -p .venv -e '.[dev]'
make up       # docker compose up -d --wait (LocalStack 3.8, PostgreSQL, MySQL)
make e2e      # tofu init + apply, scripts/e2e_hello.py, tofu destroy
```

`make e2e` exits non-zero if any check fails, or if the destroy fails. To keep the stack
for poking at it:

```bash
KEEP=1 make e2e
tofu -chdir=examples/hello output
make down     # tofu destroy; compose stays up
```

The same steps by hand:

```bash
tofu -chdir=examples/hello init
tofu -chdir=examples/hello apply \
  -var localstack_endpoint=http://localhost:4576 -var test_backoff_factor=0.01
.venv/bin/python scripts/e2e_hello.py --endpoint http://localhost:4576
tofu -chdir=examples/hello destroy \
  -var localstack_endpoint=http://localhost:4576 -var test_backoff_factor=0.01
```

`test_backoff_factor` shrinks the retry waits (30 s, 2 m, 10 m, ...) so the poison event
reaches the DLQ in seconds. It is for tests only. Leave it unset anywhere real.

LocalStack runs the function in a `public.ecr.aws/lambda/python:3.12` container via the
Docker socket. The first run pulls that image. Inside the container LocalStack sets
`AWS_ENDPOINT_URL`, and `handler.py` passes it to factbus as `endpoint_url`, because
factbus reads no environment itself.

## What the e2e script checks

| Check | Passes when |
| --- | --- |
| applied | one event on the bus gives exactly one item in each consumer's ledger, and an inbox record in each |
| duplicate | the same event put again (new EventBridge id, same CloudEvents id) leaves one item per consumer; a control event put after it is applied and both queues drain |
| independent | with `ledger`'s mapping disabled, `audit` applies a new event and `ledger` applies nothing; re-enabled, `ledger` applies it exactly once |
| delivery | real AWS only (LocalStack does not enforce queue policies, so it prints SKIP): with `audit`'s queue policy removed, the event reaches `audit`'s delivery DLQ while `ledger` applies it; the policy is restored |
| poison | the poison event reaches each processing DLQ, not the delivery DLQ, and writes no ledger item |
| evidence | each Lambda's own EMF log lines show `ConsumerDuplicates` and at least three `ConsumerRetried` |

## Run it on real AWS

Leave `localstack_endpoint` unset. The provider then uses your credential chain and
region:

```bash
tofu -chdir=examples/hello init
tofu -chdir=examples/hello apply -var prefix=my-sandbox
.venv/bin/python scripts/e2e_hello.py --endpoint '' --timeout 180
tofu -chdir=examples/hello destroy -var prefix=my-sandbox
```

The delivery arm removes one queue policy, waits `--policy-settle` (60 s) for it to take
effect, and restores it in a `finally`; if the run is killed mid-arm, `tofu apply`
restores it too. The example sets the delivery retry policy to 0 retries / 60 s so this
arm finishes in minutes; a real consumer keeps the module defaults.

Without `test_backoff_factor`, the poison arm waits out the real schedule (30 s + 2 m +
10 m) before the DLQ check can pass, so raise `--dlq-timeout` to about 900. The
function zip contains `handler.py` and `src/factbus`. boto3 comes from the Lambda runtime.
