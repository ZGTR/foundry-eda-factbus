output "bus_name" {
  value = module.bus.bus_name
}

output "bus_arn" {
  value = module.bus.bus_arn
}

output "event_source" {
  value = local.event_source
}

output "event_type" {
  value = local.event_type
}

output "producer_policy_json" {
  description = "PutEvents on this bus under `event_source` only; attached to the app role."
  value       = module.producer.policy_json
}

output "consumer" {
  value = {
    function_name = aws_lambda_function.consumer.function_name
    ledger_table  = aws_dynamodb_table.ledger.name
    inbox_table   = module.consumer_order_ledger.inbox_table_name
    queue_url     = module.consumer_order_ledger.queue_url
    queue_arn     = module.consumer_order_ledger.queue_arn
    dlq_url       = module.consumer_order_ledger.dlq_url
    mapping_uuid  = module.consumer_order_ledger.mapping_uuid
  }
}
