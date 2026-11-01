# product: the root a product copies

One bus (`module "bus"`), the producer's grant (`module "producer"`) and one consumer
(`module "consumer_order_ledger"` plus `handler.py` and its Lambda). The product's
application stages `order.placed` facts with `factbus.SqlProducer` in its own SQL
transaction; factbus publishes them after commit, and the consumer applies each one once
through its DynamoDB inbox. No notifications module: that consumer is optional.

How to adopt it, step by step, is in the repository README under
[Adopt in a product](../../README.md#adopt-in-a-product).

```bash
tofu -chdir=examples/product init -backend=false && tofu -chdir=examples/product test   # offline
make up && make e2e-product                                                          # LocalStack
```

| e2e arm | Passes when |
| --- | --- |
| staged | an order and its fact commit in one transaction, and the outbox row is marked sent by the after-commit publish |
| consumed | the consumer writes exactly one ledger item and one inbox record for the fact, and the queue drains |
| rollback | a rolled-back order leaves no order row, no outbox row and no ledger item, while a committed control order is applied |
