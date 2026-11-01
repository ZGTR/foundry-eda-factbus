output "rule_arn" {
  value = aws_cloudwatch_event_rule.this.arn
}

output "queue_arn" {
  value = aws_sqs_queue.this.arn
}

output "queue_url" {
  value = aws_sqs_queue.this.id
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}

output "delivery_dlq_arn" {
  value = aws_sqs_queue.delivery_dlq.arn
}

output "delivery_dlq_url" {
  value = aws_sqs_queue.delivery_dlq.id
}

output "mapping_uuid" {
  description = "The `<id>` in docs/kill-switch-runbook.md's disable and re-enable commands. Null while `create_event_source_mapping` is false."
  value       = one(aws_lambda_event_source_mapping.this[*].uuid)
}

output "inbox_table_name" {
  value = var.create_inbox_table ? aws_dynamodb_table.inbox[0].name : null
}

output "inbox_table_arn" {
  value = var.create_inbox_table ? aws_dynamodb_table.inbox[0].arn : null
}

output "consumer_policy_json" {
  description = "IAM policy for the consumer function's role: drain the queue and, with the inbox table, read and conditionally write it."
  value       = local.consumer_policy_json
}

output "alarm_names" {
  description = "Four alarms; three while `create_event_source_mapping` is false (no Lambda Errors alarm)."
  value = concat(
    [
      aws_cloudwatch_metric_alarm.dlq_visible.alarm_name,
      aws_cloudwatch_metric_alarm.delivery_dlq_visible.alarm_name,
      aws_cloudwatch_metric_alarm.queue_age.alarm_name,
    ],
    aws_cloudwatch_metric_alarm.lambda_errors[*].alarm_name,
  )
}
