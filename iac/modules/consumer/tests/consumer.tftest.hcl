# consumer module, offline: a mock AWS provider and plan-only runs, with fixed ARNs where
# an assertion compares one resource's ARN with another's policy. No AWS credentials and
# no AWS call.

mock_provider "aws" {}

override_resource {
  target = aws_sqs_queue.this
  values = {
    arn = "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
    id  = "https://sqs.eu-west-1.amazonaws.com/111111111111/event-ledger"
  }
}

override_resource {
  target = aws_sqs_queue.dlq
  values = { arn = "arn:aws:sqs:eu-west-1:111111111111:event-ledger-dlq" }
}

override_resource {
  target = aws_sqs_queue.delivery_dlq
  values = {
    arn = "arn:aws:sqs:eu-west-1:111111111111:event-ledger-delivery-dlq"
    id  = "https://sqs.eu-west-1.amazonaws.com/111111111111/event-ledger-delivery-dlq"
  }
}

override_resource {
  target = aws_cloudwatch_event_rule.this
  values = { arn = "arn:aws:events:eu-west-1:111111111111:rule/example-events/event-ledger" }
}

override_resource {
  target = aws_dynamodb_table.inbox
  values = { arn = "arn:aws:dynamodb:eu-west-1:111111111111:table/event-ledger-inbox" }
}

# The ten-line call from the phase-1 plan (2026-10-23-eda-kit-phase1.md), less the module `source` and alarm topic.
variables {
  name         = "event-ledger"
  bus_name     = "example-events"
  sources      = ["example.producer"]
  event_types  = ["learning.byte.completed"]
  function_arn = "arn:aws:lambda:eu-west-1:111111111111:function:event-ledger"
}

run "rule_matches_sources_and_types_and_targets_the_queue" {
  command = plan
  assert {
    condition = jsondecode(aws_cloudwatch_event_rule.this[0].event_pattern) == {
      source        = ["example.producer"]
      "detail-type" = ["learning.byte.completed"]
    }
    error_message = "the rule must match exactly the configured sources and detail-types: ${aws_cloudwatch_event_rule.this[0].event_pattern}"
  }
  assert {
    condition     = aws_cloudwatch_event_rule.this[0].event_bus_name == "example-events" && aws_cloudwatch_event_target.this[0].event_bus_name == "example-events"
    error_message = "rule and target must be on bus_name"
  }
  assert {
    condition     = aws_cloudwatch_event_target.this[0].arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
    error_message = "the rule must target this module's queue"
  }
}

# detail_pattern narrows the rule on `detail`. The rendered pattern must equal the shared
# fixture's, which tests/unit and tests/integration feed to EventBridge's TestEventPattern
# (moto and LocalStack) to prove the whatsapp event matches and the email one does not.
# The run above is the control: with detail_pattern null the rule carries no `detail`.
run "detail_pattern_is_merged_into_the_rule" {
  command = plan
  variables {
    detail_pattern = { channel = ["whatsapp"] }
  }
  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.this[0].event_pattern) == jsondecode(file("${path.module}/tests/fixtures/detail_pattern.json")).pattern
    error_message = "the rule must carry source, detail-type and the detail filter: ${aws_cloudwatch_event_rule.this[0].event_pattern}"
  }
  assert {
    condition = (
      contains(
        jsondecode(aws_cloudwatch_event_rule.this[0].event_pattern).detail.channel,
        jsondecode(file("${path.module}/tests/fixtures/detail_pattern.json")).matching_event.detail.channel,
      )
      && !contains(
        jsondecode(aws_cloudwatch_event_rule.this[0].event_pattern).detail.channel,
        jsondecode(file("${path.module}/tests/fixtures/detail_pattern.json")).non_matching_event.detail.channel,
      )
    )
    error_message = "a rule for channel=whatsapp must admit the whatsapp event's channel and not the email event's"
  }
}

run "an_empty_detail_pattern_is_refused" {
  command = plan
  variables {
    detail_pattern = {}
  }
  expect_failures = [var.detail_pattern]
}

run "queue_policy_admits_only_the_rule" {
  command = plan
  assert {
    condition     = length(jsondecode(aws_sqs_queue_policy.this.policy).Statement) == 1
    error_message = "the queue policy must carry exactly one statement"
  }
  assert {
    condition = (
      one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Condition
      == { ArnEquals = { "aws:SourceArn" = "arn:aws:events:eu-west-1:111111111111:rule/example-events/event-ledger" } }
    )
    error_message = "the policy's only condition must be ArnEquals aws:SourceArn = this rule's ARN: ${aws_sqs_queue_policy.this.policy}"
  }
  assert {
    condition = (
      one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Principal == { Service = "events.amazonaws.com" }
      && one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Action == "sqs:SendMessage"
      && one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Resource == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
      && !strcontains(aws_sqs_queue_policy.this.policy, "*")
    )
    error_message = "the policy must grant events.amazonaws.com sqs:SendMessage on this queue only, with no wildcard"
  }
}

run "failed_delivery_goes_to_the_delivery_dlq" {
  command = plan
  assert {
    condition     = one(aws_cloudwatch_event_target.this[0].dead_letter_config).arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger-delivery-dlq"
    error_message = "the target must dead-letter to this module's delivery DLQ"
  }
  assert {
    condition     = aws_sqs_queue.delivery_dlq.name == "event-ledger-delivery-dlq" && aws_sqs_queue.delivery_dlq.arn != aws_sqs_queue.dlq.arn
    error_message = "the delivery DLQ must be its own queue, distinct from the processing DLQ"
  }
  assert {
    condition = (
      one(aws_cloudwatch_event_target.this[0].retry_policy).maximum_retry_attempts == 185
      && one(aws_cloudwatch_event_target.this[0].retry_policy).maximum_event_age_in_seconds == 86400
    )
    error_message = "the target retry policy must default to the EventBridge maximum (185 attempts, 24 h)"
  }
  assert {
    condition = (
      aws_cloudwatch_metric_alarm.delivery_dlq_visible.dimensions["QueueName"] == "event-ledger-delivery-dlq"
      && aws_cloudwatch_metric_alarm.delivery_dlq_visible.threshold == 0
    )
    error_message = "a visible delivery DLQ message must alarm"
  }
}

run "delivery_dlq_policy_admits_only_the_rule" {
  command = plan
  assert {
    condition = (
      length(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement) == 1
      && one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Principal == { Service = "events.amazonaws.com" }
      && one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Action == "sqs:SendMessage"
      && one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Resource == "arn:aws:sqs:eu-west-1:111111111111:event-ledger-delivery-dlq"
      && one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Condition == { ArnEquals = { "aws:SourceArn" = "arn:aws:events:eu-west-1:111111111111:rule/example-events/event-ledger" } }
      && !strcontains(aws_sqs_queue_policy.delivery_dlq.policy, "*")
    )
    error_message = "the delivery DLQ policy must admit only this rule: ${aws_sqs_queue_policy.delivery_dlq.policy}"
  }
}

run "delivery_retry_policy_follows_variables" {
  command = plan
  variables {
    delivery_max_retry_attempts    = 0
    delivery_max_event_age_seconds = 60
  }
  assert {
    condition = (
      one(aws_cloudwatch_event_target.this[0].retry_policy).maximum_retry_attempts == 0
      && one(aws_cloudwatch_event_target.this[0].retry_policy).maximum_event_age_in_seconds == 60
    )
    error_message = "the retry policy must follow delivery_max_retry_attempts and delivery_max_event_age_seconds"
  }
}

run "delivery_retry_out_of_range_is_refused" {
  command = plan
  variables {
    delivery_max_retry_attempts = 186
  }
  expect_failures = [var.delivery_max_retry_attempts]
}

run "redrive_defaults_to_ten_receives" {
  command = plan
  assert {
    condition = jsondecode(aws_sqs_queue.this.redrive_policy) == {
      deadLetterTargetArn = "arn:aws:sqs:eu-west-1:111111111111:event-ledger-dlq"
      maxReceiveCount     = 10
    }
    error_message = "redrive must send to the DLQ after 10 receives: ${aws_sqs_queue.this.redrive_policy}"
  }
}

run "redrive_follows_max_receive_count" {
  command = plan
  variables {
    max_receive_count = 3
  }
  assert {
    condition     = jsondecode(aws_sqs_queue.this.redrive_policy).maxReceiveCount == 3
    error_message = "max_receive_count must reach the redrive policy"
  }
}

run "mapping_reports_batch_item_failures" {
  command = plan
  variables {
    batch_size                         = 25
    maximum_batching_window_in_seconds = 5
  }
  assert {
    condition     = contains(aws_lambda_event_source_mapping.this[0].function_response_types, "ReportBatchItemFailures")
    error_message = "the mapping must report partial batch failures"
  }
  assert {
    condition = (
      aws_lambda_event_source_mapping.this[0].enabled == true
      && aws_lambda_event_source_mapping.this[0].batch_size == 25
      && aws_lambda_event_source_mapping.this[0].maximum_batching_window_in_seconds == 5
      && aws_lambda_event_source_mapping.this[0].event_source_arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
    )
    error_message = "the mapping must be on, read this queue and carry the batch settings"
  }
}

run "mapping_is_uncapped_by_default" {
  command = plan
  assert {
    condition     = length(aws_lambda_event_source_mapping.this[0].scaling_config) == 0
    error_message = "with maximum_concurrency null the mapping must carry no scaling_config"
  }
}

run "mapping_concurrency_follows_maximum_concurrency" {
  command = plan
  variables {
    maximum_concurrency = 5
  }
  assert {
    condition     = one(aws_lambda_event_source_mapping.this[0].scaling_config).maximum_concurrency == 5
    error_message = "maximum_concurrency must reach the mapping's scaling_config"
  }
}

run "mapping_concurrency_below_two_is_refused" {
  command = plan
  variables {
    maximum_concurrency = 1
  }
  expect_failures = [var.maximum_concurrency]
}

run "kill_switch_disables_the_mapping_only" {
  command = plan
  variables {
    enabled = false
  }
  assert {
    condition     = aws_lambda_event_source_mapping.this[0].enabled == false
    error_message = "enabled = false must disable the event source mapping"
  }
  assert {
    condition     = aws_sqs_queue.this.name == "event-ledger" && aws_cloudwatch_event_rule.this[0].name == "event-ledger"
    error_message = "the kill switch must leave the queue and rule in place, so events wait rather than vanish"
  }
}

# Queue-first deploy (Shepherd's infra/events): the queue exists and is an output before
# the function does, so there is no mapping to create and no function for the Lambda
# Errors alarm to name.
run "without_the_mapping_the_queue_is_still_created_and_output" {
  command = plan
  variables {
    create_event_source_mapping = false
    function_arn                = ""
  }
  assert {
    condition     = length(aws_lambda_event_source_mapping.this) == 0 && output.mapping_uuid == null
    error_message = "create_event_source_mapping = false must create no event source mapping"
  }
  assert {
    condition     = length(aws_cloudwatch_metric_alarm.lambda_errors) == 0
    error_message = "create_event_source_mapping = false must create no Lambda Errors alarm (it would name an empty function)"
  }
  assert {
    condition = (
      output.queue_arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
      && aws_sqs_queue.this.name == "event-ledger"
      && aws_cloudwatch_event_target.this[0].arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
    )
    error_message = "the queue, its rule target and the queue_arn output must exist without the mapping"
  }
  assert {
    condition     = output.alarm_names == ["event-ledger-dlq-visible", "event-ledger-delivery-dlq-visible", "event-ledger-age"]
    error_message = "the three queue alarms stay; only the Lambda Errors alarm goes: ${jsonencode(output.alarm_names)}"
  }
}

# Positive control for the run above: by default both exist.
run "by_default_the_mapping_and_the_errors_alarm_exist" {
  command = plan
  assert {
    condition     = length(aws_lambda_event_source_mapping.this) == 1 && length(aws_cloudwatch_metric_alarm.lambda_errors) == 1
    error_message = "the default must create the event source mapping and the Lambda Errors alarm"
  }
  assert {
    condition     = length(output.alarm_names) == 4 && contains(output.alarm_names, "event-ledger-errors")
    error_message = "the default must report four alarms, including the Lambda Errors alarm"
  }
}

run "an_empty_function_with_the_mapping_is_refused" {
  command = plan
  variables {
    function_arn = ""
  }
  expect_failures = [aws_lambda_event_source_mapping.this]
}

run "alarms_notify_the_topic_when_given" {
  command = plan
  variables {
    alarm_topic_arn = "arn:aws:sns:eu-west-1:111111111111:ops"
  }
  assert {
    condition = (
      aws_cloudwatch_metric_alarm.dlq_visible.alarm_actions == toset(["arn:aws:sns:eu-west-1:111111111111:ops"])
      && aws_cloudwatch_metric_alarm.queue_age.alarm_actions == toset(["arn:aws:sns:eu-west-1:111111111111:ops"])
      && aws_cloudwatch_metric_alarm.lambda_errors[0].alarm_actions == toset(["arn:aws:sns:eu-west-1:111111111111:ops"])
      && aws_cloudwatch_metric_alarm.delivery_dlq_visible.alarm_actions == toset(["arn:aws:sns:eu-west-1:111111111111:ops"])
    )
    error_message = "every alarm must notify alarm_topic_arn"
  }
  assert {
    condition = (
      aws_cloudwatch_metric_alarm.dlq_visible.metric_name == "ApproximateNumberOfMessagesVisible"
      && aws_cloudwatch_metric_alarm.dlq_visible.threshold == 0
      && aws_cloudwatch_metric_alarm.dlq_visible.comparison_operator == "GreaterThanThreshold"
      && aws_cloudwatch_metric_alarm.queue_age.metric_name == "ApproximateAgeOfOldestMessage"
      && aws_cloudwatch_metric_alarm.lambda_errors[0].namespace == "AWS/Lambda"
      && aws_cloudwatch_metric_alarm.lambda_errors[0].dimensions["FunctionName"] == "event-ledger" && length(aws_cloudwatch_metric_alarm.lambda_errors[0].dimensions) == 1
    )
    error_message = "alarms: DLQ visible > 0, queue age, and Lambda Errors on the function's name"
  }
}

run "alarms_without_a_topic_notify_nobody" {
  command = plan
  assert {
    condition     = length(aws_cloudwatch_metric_alarm.dlq_visible.alarm_actions) == 0
    error_message = "no alarm_topic_arn must mean no alarm action"
  }
}

run "inbox_table_shape" {
  command = plan
  assert {
    condition = (
      aws_dynamodb_table.inbox[0].name == "event-ledger-inbox"
      && aws_dynamodb_table.inbox[0].billing_mode == "PAY_PER_REQUEST"
      && aws_dynamodb_table.inbox[0].hash_key == "pk"
      && aws_dynamodb_table.inbox[0].range_key == "sk"
      && aws_dynamodb_table.inbox[0].attribute == toset([{ name = "pk", type = "S" }, { name = "sk", type = "S" }])
    )
    error_message = "the inbox must be on-demand with string pk/sk keys"
  }
  assert {
    condition     = aws_dynamodb_table.inbox[0].ttl[0].attribute_name == "expires_at" && aws_dynamodb_table.inbox[0].ttl[0].enabled
    error_message = "the inbox must expire items on expires_at"
  }
  assert {
    condition     = aws_dynamodb_table.inbox[0].point_in_time_recovery[0].enabled
    error_message = "the inbox must have point-in-time recovery on"
  }
  assert {
    condition     = output.inbox_table_name == "event-ledger-inbox" && output.inbox_table_arn == "arn:aws:dynamodb:eu-west-1:111111111111:table/event-ledger-inbox"
    error_message = "the inbox table's name and ARN must be outputs"
  }
}

run "consumer_policy_covers_queue_and_inbox" {
  command = plan
  assert {
    condition = {
      for s in jsondecode(output.consumer_policy_json).Statement : s.Sid => s.Resource
      } == {
      DrainQueue = "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
      UseInbox   = "arn:aws:dynamodb:eu-west-1:111111111111:table/event-ledger-inbox"
    }
    error_message = "the consumer policy must name this queue and this inbox table, nothing else"
  }
  assert {
    condition = alltrue([
      for a in ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "dynamodb:PutItem", "dynamodb:GetItem"] :
      contains(flatten([for s in jsondecode(output.consumer_policy_json).Statement : s.Action]), a)
    ]) && !strcontains(output.consumer_policy_json, "*")
    error_message = "the consumer policy is missing an action the consumer needs, or carries a wildcard"
  }
  assert {
    condition     = length(aws_iam_role_policy.consumer) == 0
    error_message = "no role_name must mean no attachment"
  }
}

run "policy_attaches_to_role_name" {
  command = plan
  variables {
    role_name = "event-ledger-fn"
  }
  assert {
    condition     = aws_iam_role_policy.consumer[0].role == "event-ledger-fn" && aws_iam_role_policy.consumer[0].policy == output.consumer_policy_json
    error_message = "role_name must receive consumer_policy_json"
  }
}

run "no_inbox_table_when_not_asked" {
  command = plan
  variables {
    create_inbox_table = false
  }
  assert {
    condition = (
      length(aws_dynamodb_table.inbox) == 0 && output.inbox_table_arn == null
      && [for s in jsondecode(output.consumer_policy_json).Statement : s.Sid] == ["DrainQueue"]
    )
    error_message = "create_inbox_table = false must create no table and grant no table action"
  }
}

run "empty_sources_are_refused" {
  command = plan
  variables {
    sources = []
  }
  expect_failures = [var.sources]
}

run "empty_event_types_are_refused" {
  command = plan
  variables {
    event_types = []
  }
  expect_failures = [var.event_types]
}

# No VPC, subnet, NAT gateway or database instance. tofu test cannot enumerate a plan's
# resource types, so this reads the module's source: it declares every resource in its
# own .tf files and calls no child module, so the set of resource types in those files is
# the set the plan can contain.
run "no_network_or_database_resources" {
  command = plan
  assert {
    condition = alltrue([
      for f in fileset(path.module, "*.tf") :
      length(regexall("resource\\s+\"aws_(vpc|subnet|nat_gateway|db_instance|rds_cluster|internet_gateway|eip)\"", file("${path.module}/${f}"))) == 0
    ])
    error_message = "the consumer module must declare no VPC, subnet, NAT gateway or database instance"
  }
  assert {
    condition = alltrue([
      for f in fileset(path.module, "*.tf") :
      length(regexall("(?m)^\\s*module\\s+\"", file("${path.module}/${f}"))) == 0
    ]) && length(fileset(path.module, "*.tf")) == 4
    error_message = "the consumer module must call no child module, or the source scan above proves nothing"
  }
  assert {
    condition = alltrue([
      for f in fileset(path.module, "*.tf") :
      length(regexall("resource\\s+\"aws_dynamodb_table\"", file("${path.module}/${f}"))) <= 1
    ]) && length(regexall("resource\\s+\"aws_dynamodb_table\"", file("${path.module}/main.tf"))) == 1
    error_message = "positive control: the scan must see the one resource type the module does declare"
  }
}

# --- subscriptions: several buses, one queue ----------------------------------------------
# Notifications' results queue hears delivery results on its own bus and the product's
# answers on the product's bus, with one kill switch. These runs apply against the mock
# provider so the rules have ARNs. The mock gives every rule the same generated ARN (and
# override_resource cannot target one instance), so the policy runs check one entry per
# rule and that each entry is a rule ARN; the LocalStack apply in the notifications
# module proves the distinct ARNs.

run "subscriptions_make_one_rule_per_entry_on_its_own_bus" {
  command = apply
  variables {
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "results", bus_name = "comms-events", event_pattern = jsonencode({ source = [{ "anything-but" = ["comms-core"] }], "detail-type" = ["comms.delivery.accepted"] }) },
      { key = "answers", bus_name = "product-events", event_pattern = jsonencode({ source = ["product-backend"], "detail-type" = ["product.action.completed"] }) },
    ]
  }
  assert {
    condition     = length(aws_cloudwatch_event_rule.this) == 0 && length(aws_cloudwatch_event_target.this) == 0 && output.rule_arn == null
    error_message = "subscriptions must replace the single rule, not add to it"
  }
  assert {
    condition = (
      { for k, r in aws_cloudwatch_event_rule.subscription : k => [r.name, r.event_bus_name] }
      == { results = ["event-ledger-results", "comms-events"], answers = ["event-ledger-answers", "product-events"] }
    )
    error_message = "each entry must be a rule named <name>-<key> on its own bus"
  }
  assert {
    condition = (
      jsondecode(aws_cloudwatch_event_rule.subscription["results"].event_pattern) == { source = [{ "anything-but" = ["comms-core"] }], "detail-type" = ["comms.delivery.accepted"] }
      && jsondecode(aws_cloudwatch_event_rule.subscription["answers"].event_pattern) == { source = ["product-backend"], "detail-type" = ["product.action.completed"] }
    )
    error_message = "each rule must carry its entry's pattern as written"
  }
  assert {
    condition = alltrue([
      for k, t in aws_cloudwatch_event_target.subscription :
      t.arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger"
      && t.event_bus_name == aws_cloudwatch_event_rule.subscription[k].event_bus_name
      && one(t.dead_letter_config).arn == "arn:aws:sqs:eu-west-1:111111111111:event-ledger-delivery-dlq"
      && one(t.retry_policy).maximum_retry_attempts == 185
    ]) && length(aws_cloudwatch_event_target.subscription) == 2
    error_message = "every rule must target the one queue on its own bus, with the delivery DLQ and retry policy"
  }
  assert {
    condition     = output.rule_arns == { for k, r in aws_cloudwatch_event_rule.subscription : k => r.arn }
    error_message = "rule_arns must name every subscription's rule by key"
  }
}

run "one_queue_policy_admits_every_subscription_rule_and_nothing_else" {
  command = apply
  variables {
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "results", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["comms.delivery.accepted"] }) },
      { key = "answers", bus_name = "product-events", event_pattern = jsonencode({ "detail-type" = ["product.action.completed"] }) },
      { key = "inbound", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["comms.inbound.received"] }) },
    ]
  }
  assert {
    condition = (
      length(jsondecode(aws_sqs_queue_policy.this.policy).Statement) == 1
      && toset(one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Condition.ArnEquals["aws:SourceArn"])
      == toset([for r in aws_cloudwatch_event_rule.subscription : r.arn])
      && length(one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Condition.ArnEquals["aws:SourceArn"]) == 3
      && one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Action == "sqs:SendMessage"
      && !strcontains(aws_sqs_queue_policy.this.policy, "*")
    )
    error_message = "the queue's one policy must admit exactly the three rules: ${aws_sqs_queue_policy.this.policy}"
  }
  assert {
    condition = (
      length(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement) == 1
      && toset(one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Condition.ArnEquals["aws:SourceArn"])
      == toset([for r in aws_cloudwatch_event_rule.subscription : r.arn])
      && length(one(jsondecode(aws_sqs_queue_policy.delivery_dlq.policy).Statement).Condition.ArnEquals["aws:SourceArn"]) == 3
    )
    error_message = "the delivery DLQ policy must admit exactly the three rules: ${aws_sqs_queue_policy.delivery_dlq.policy}"
  }
}

# Control for the two runs above: one rule keeps the string form, so the single-bus
# policy is unchanged by `subscriptions` (queue_policy_admits_only_the_rule compares it
# with the fixed rule ARN).
run "one_subscription_keeps_the_string_condition" {
  command = apply
  variables {
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "only", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["comms.delivery.accepted"] }) },
    ]
  }
  assert {
    condition     = one(jsondecode(aws_sqs_queue_policy.this.policy).Statement).Condition.ArnEquals["aws:SourceArn"] == aws_cloudwatch_event_rule.subscription["only"].arn
    error_message = "a single subscription must be admitted by its ARN as a string"
  }
}

run "subscriptions_beside_the_single_bus_form_are_refused" {
  command = plan
  variables {
    subscriptions = [
      { key = "results", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["comms.delivery.accepted"] }) },
    ]
  }
  expect_failures = [aws_sqs_queue.this]
}

run "neither_form_is_refused" {
  command = plan
  variables {
    bus_name    = null
    sources     = null
    event_types = null
  }
  expect_failures = [aws_sqs_queue.this]
}

run "duplicate_subscription_keys_are_refused" {
  command = plan
  variables {
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "results", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["a"] }) },
      { key = "results", bus_name = "product-events", event_pattern = jsonencode({ "detail-type" = ["b"] }) },
    ]
  }
  expect_failures = [var.subscriptions]
}

run "a_subscription_pattern_that_is_not_a_json_object_is_refused" {
  command = plan
  variables {
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "results", bus_name = "comms-events", event_pattern = "detail-type=a" },
    ]
  }
  expect_failures = [var.subscriptions]
}

run "a_subscription_rule_name_over_64_characters_is_refused" {
  command = plan
  variables {
    name        = "a-consumer-name-that-is-forty-characters"
    bus_name    = null
    sources     = null
    event_types = null
    subscriptions = [
      { key = "a-key-of-twenty-eight-chars-", bus_name = "comms-events", event_pattern = jsonencode({ "detail-type" = ["a"] }) },
    ]
  }
  expect_failures = [aws_cloudwatch_event_rule.subscription]
}
