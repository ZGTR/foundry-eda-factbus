# One consumer's whole AWS side in one module block: an EventBridge rule on the
# producer's bus, the queue and DLQ it delivers to, the delivery DLQ that catches events
# the rule could not put on the queue, the Lambda event source mapping that drains the
# queue, four alarms and, optionally, the DynamoDB inbox table the kit uses to apply each
# event once. No VPC, subnet, NAT gateway or database instance: the inbox
# is DynamoDB, so a consumer needs no network plumbing.
#
# One bus and one rule per consumer, so a caller writes about ten lines. A consumer that
# must hear several buses on ONE queue (one kill switch, one ordering point) sets
# `subscriptions` instead: one rule per entry, each on its own bus, all targeting the same
# queue, and one queue policy admitting every rule.

locals {
  # The function name, for the Lambda Errors alarm. A full ARN carries it as the 7th
  # field; a bare name or a qualified ARN is handled too.
  function_name = (
    length(split(":", var.function_arn)) >= 7 ? split(":", var.function_arn)[6] : var.function_arn
  )
  alarm_actions    = var.alarm_topic_arn == null ? [] : [var.alarm_topic_arn]
  inbox_table_name = coalesce(var.inbox_table_name, "${var.name}-inbox")

  # Single-bus form (`bus_name`, `sources`, `event_types`) or `subscriptions`, never both.
  from_subscriptions = length(var.subscriptions) > 0
  subscriptions      = { for s in var.subscriptions : s.key => s }

  # Every rule this module creates, in a stable order: the single rule, or the
  # subscriptions sorted by key. Both queue policies admit exactly these.
  rule_arns = concat(
    aws_cloudwatch_event_rule.this[*].arn,
    [for key in sort(keys(aws_cloudwatch_event_rule.subscription)) : aws_cloudwatch_event_rule.subscription[key].arn],
  )
  # One rule keeps the policy's string form, so a single-bus caller's policy document is
  # unchanged by `subscriptions`; several rules are a list. jsondecode(jsonencode()) gives
  # both branches one type.
  source_arn_condition = jsondecode(length(local.rule_arns) == 1 ? jsonencode(local.rule_arns[0]) : jsonencode(local.rule_arns))
}

# `detail_pattern`, when set, narrows the rule on the event's `detail` (for example
# `{ channel = ["whatsapp"] }`). Null leaves the pattern exactly as before: source and
# detail-type only.
resource "aws_cloudwatch_event_rule" "this" {
  count          = local.from_subscriptions ? 0 : 1
  name           = var.name
  event_bus_name = var.bus_name
  event_pattern = jsonencode(merge(
    {
      source        = var.sources
      "detail-type" = var.event_types
    },
    { for key, value in { detail = var.detail_pattern } : key => value if value != null },
  ))
}

# Two failure boundaries, two DLQs. EventBridge retries a failed put to the queue (a
# denied queue policy, a deleted queue) and then parks the event on the delivery DLQ;
# the queue's redrive moves a message the handler keeps failing to the processing DLQ.
resource "aws_cloudwatch_event_target" "this" {
  count          = local.from_subscriptions ? 0 : 1
  rule           = aws_cloudwatch_event_rule.this[0].name
  event_bus_name = var.bus_name
  arn            = aws_sqs_queue.this.arn

  retry_policy {
    maximum_event_age_in_seconds = var.delivery_max_event_age_seconds
    maximum_retry_attempts       = var.delivery_max_retry_attempts
  }

  dead_letter_config {
    arn = aws_sqs_queue.delivery_dlq.arn
  }

  # EventBridge dead-letters at once when it lacks permission, so the target must not
  # go live before either queue admits the rule.
  depends_on = [aws_sqs_queue_policy.this, aws_sqs_queue_policy.delivery_dlq]
}

# `subscriptions`: one rule per entry, named `<name>-<key>`, on the entry's own bus, with
# the entry's own pattern. Keyed, so adding or removing one entry moves no other. Each
# target has the same retry policy and delivery DLQ as the single rule's.
resource "aws_cloudwatch_event_rule" "subscription" {
  for_each       = local.subscriptions
  name           = "${var.name}-${each.key}"
  event_bus_name = each.value.bus_name
  event_pattern  = each.value.event_pattern

  lifecycle {
    precondition {
      condition     = length("${var.name}-${each.key}") <= 64
      error_message = "consumer ${var.name}: rule name ${var.name}-${each.key} is longer than EventBridge's 64 characters; shorten name or the key."
    }
  }
}

resource "aws_cloudwatch_event_target" "subscription" {
  for_each       = local.subscriptions
  rule           = aws_cloudwatch_event_rule.subscription[each.key].name
  event_bus_name = each.value.bus_name
  arn            = aws_sqs_queue.this.arn

  retry_policy {
    maximum_event_age_in_seconds = var.delivery_max_event_age_seconds
    maximum_retry_attempts       = var.delivery_max_retry_attempts
  }

  dead_letter_config {
    arn = aws_sqs_queue.delivery_dlq.arn
  }

  depends_on = [aws_sqs_queue_policy.this, aws_sqs_queue_policy.delivery_dlq]
}

# Both were unconditional before `subscriptions` existed; a stack applied then keeps its
# rule and target instead of replacing them.
moved {
  from = aws_cloudwatch_event_rule.this
  to   = aws_cloudwatch_event_rule.this[0]
}

moved {
  from = aws_cloudwatch_event_target.this
  to   = aws_cloudwatch_event_target.this[0]
}

resource "aws_sqs_queue" "delivery_dlq" {
  name                      = "${var.name}-delivery-dlq"
  message_retention_seconds = var.retention_seconds
}

# EventBridge writes the delivery DLQ as the rule, so the grant names the rule (every
# rule, with `subscriptions`), as on the queue.
resource "aws_sqs_queue_policy" "delivery_dlq" {
  queue_url = aws_sqs_queue.delivery_dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgeRuleDeadLetter"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.delivery_dlq.arn
      Condition = { ArnEquals = { "aws:SourceArn" = local.source_arn_condition } }
    }]
  })
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = var.retention_seconds
}

resource "aws_sqs_queue" "this" {
  name                       = var.name
  message_retention_seconds  = var.retention_seconds
  visibility_timeout_seconds = var.function_timeout_seconds * 6

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  lifecycle {
    precondition {
      condition = (
        local.from_subscriptions
        ? var.bus_name == null && var.sources == null && var.event_types == null && var.detail_pattern == null
        : var.bus_name != null && var.sources != null && var.event_types != null
      )
      error_message = "consumer ${var.name}: set bus_name, sources and event_types (one bus), or subscriptions (several), not both."
    }
  }
}

# The queue's only policy. It admits this module's rules and nothing else: no wildcard,
# no account-wide grant, no ARN from outside. An SQS queue has one policy, so every
# subscription's grant lives here; a second `aws_sqs_queue_policy` would overwrite it.
resource "aws_sqs_queue_policy" "this" {
  queue_url = aws_sqs_queue.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgeRule"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.this.arn
      Condition = { ArnEquals = { "aws:SourceArn" = local.source_arn_condition } }
    }]
  })
}

# `enabled` is the kill switch (docs/kill-switch-runbook.md). Setting it false in the
# caller's IaC keeps a thrown switch off across later applies; messages wait on the
# queue for up to `retention_seconds`.
#
# `create_event_source_mapping = false` is for a caller that deploys queue-first: it
# applies this module before the function exists, reads `queue_arn`, deploys the
# function, then applies again with the flag true. Until then there is no mapping and
# no Lambda Errors alarm (which would name an empty function).
resource "aws_lambda_event_source_mapping" "this" {
  count                              = var.create_event_source_mapping ? 1 : 0
  event_source_arn                   = aws_sqs_queue.this.arn
  function_name                      = var.function_arn
  enabled                            = var.enabled
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.maximum_batching_window_in_seconds
  function_response_types            = ["ReportBatchItemFailures"]

  dynamic "scaling_config" {
    for_each = var.maximum_concurrency == null ? [] : [var.maximum_concurrency]
    content {
      maximum_concurrency = scaling_config.value
    }
  }

  lifecycle {
    precondition {
      condition     = length(trimspace(var.function_arn)) > 0
      error_message = "function_arn must name the consumer function when create_event_source_mapping is true."
    }
  }
}

# Both resources were unconditional before `create_event_source_mapping` existed; a
# stack applied then keeps its mapping and alarm instead of replacing them.
moved {
  from = aws_lambda_event_source_mapping.this
  to   = aws_lambda_event_source_mapping.this[0]
}

moved {
  from = aws_cloudwatch_metric_alarm.lambda_errors
  to   = aws_cloudwatch_metric_alarm.lambda_errors[0]
}

resource "aws_cloudwatch_metric_alarm" "dlq_visible" {
  alarm_name          = "${var.name}-dlq-visible"
  alarm_description   = "A message reached ${var.name}'s DLQ after ${var.max_receive_count} receives."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "delivery_dlq_visible" {
  alarm_name          = "${var.name}-delivery-dlq-visible"
  alarm_description   = "EventBridge could not deliver an event to ${var.name} and parked it on the delivery DLQ."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.delivery_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "queue_age" {
  alarm_name          = "${var.name}-age"
  alarm_description   = "The oldest message on ${var.name} is older than ${var.age_alarm_threshold_seconds} s."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.this.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.age_alarm_threshold_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  count               = var.create_event_source_mapping ? 1 : 0
  alarm_name          = "${var.name}-errors"
  alarm_description   = "The function draining ${var.name} raised an error."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = local.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# The inbox: one item per applied event, written with a conditional put so a redelivery
# is recognised and skipped. `expires_at` (epoch seconds) lets DynamoDB drop old keys.
resource "aws_dynamodb_table" "inbox" {
  count        = var.create_inbox_table ? 1 : 0
  name         = local.inbox_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

locals {
  consumer_policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid    = "DrainQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
        ]
        Resource = aws_sqs_queue.this.arn
      }],
      var.create_inbox_table ? [{
        Sid    = "UseInbox"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:ConditionCheckItem",
        ]
        Resource = aws_dynamodb_table.inbox[0].arn
      }] : [],
    )
  })
}

# Optional: attach the policy to the consumer function's role. A role name written in
# the caller's config is known at plan; a generated one (name_prefix) is not, and then
# the caller attaches `consumer_policy_json` itself.
resource "aws_iam_role_policy" "consumer" {
  count  = var.role_name == null ? 0 : 1
  name   = "${var.name}-consumer"
  role   = var.role_name
  policy = local.consumer_policy_json
}
