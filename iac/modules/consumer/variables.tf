variable "name" {
  description = "Base name for the rule, the queue (`<name>`), its DLQ (`<name>-dlq`), the delivery DLQ (`<name>-delivery-dlq`), the alarms and, by default, the inbox table (`<name>-inbox`)."
  type        = string
  validation {
    # SQS allows 80 characters; `-delivery-dlq` needs room.
    condition     = can(regex("^[A-Za-z0-9_-]{1,64}$", var.name))
    error_message = "name: 1 to 64 letters, digits, '-' or '_' (it names an SQS queue and an EventBridge rule)."
  }
}

variable "bus_name" {
  description = "The EventBridge bus the producer publishes to (the `bus` module's `bus_name` output)."
  type        = string
  validation {
    condition     = length(var.bus_name) > 0
    error_message = "bus_name must not be empty: an empty name subscribes to the account's default bus."
  }
}

variable "sources" {
  description = "CloudEvents `source` values this consumer accepts, matched against EventBridge `source`."
  type        = list(string)
  validation {
    condition     = length(var.sources) > 0
    error_message = "sources: at least one; an empty list would match nothing."
  }
}

variable "event_types" {
  description = "CloudEvents `type` values this consumer accepts, matched against EventBridge `detail-type`."
  type        = list(string)
  validation {
    condition     = length(var.event_types) > 0
    error_message = "event_types: at least one; an empty list would match nothing."
  }
}

variable "detail_pattern" {
  description = <<-EOT
    Optional EventBridge pattern on the event's `detail`, merged into the rule beside
    `source` and `detail-type`, for example `{ channel = ["whatsapp"] }`. Values follow
    EventBridge pattern syntax (lists of allowed values, nested objects, content filters).
    Null, the default, matches on source and detail-type only.
  EOT
  type        = any
  default     = null
  validation {
    condition     = var.detail_pattern == null || try(length(keys(var.detail_pattern)) > 0, false)
    error_message = "detail_pattern must be null or a non-empty object of detail fields."
  }
}

variable "function_arn" {
  description = "The Lambda function that drains the queue (ARN or name). May be empty only while `create_event_source_mapping` is false."
  type        = string
}

variable "create_event_source_mapping" {
  description = <<-EOT
    Create the event source mapping and the Lambda Errors alarm. False for a queue-first
    deploy: the rule, queue, DLQs, their alarms and the inbox exist and `queue_arn` is an
    output, but nothing invokes a function yet, so `function_arn` may be empty. Apply
    again with true once the function exists. Not the kill switch: that is `enabled`.
  EOT
  type        = bool
  default     = true
}

variable "enabled" {
  description = <<-EOT
    The kill switch: the event source mapping's Enabled flag. False stops new receives;
    messages wait on the queue (docs/kill-switch-runbook.md). Set it here, not only with
    the CLI, so the next apply does not turn a thrown switch back on.
  EOT
  type        = bool
  default     = true
}

variable "batch_size" {
  description = "Messages per Lambda invocation."
  type        = number
  default     = 10
}

variable "maximum_batching_window_in_seconds" {
  description = "Seconds the poller may wait to fill a batch. 0 invokes as soon as a message arrives."
  type        = number
  default     = 0
}

variable "maximum_concurrency" {
  description = "Cap on concurrent invocations the mapping drives (ScalingConfig.MaximumConcurrency, 2-1000). Null leaves the mapping uncapped. Set it at or below the function's reserved concurrency, so throttled batches do not burn receives toward the DLQ."
  type        = number
  default     = null
  validation {
    condition     = var.maximum_concurrency == null || (try(var.maximum_concurrency >= 2 && var.maximum_concurrency <= 1000 && floor(var.maximum_concurrency) == var.maximum_concurrency, false))
    error_message = "maximum_concurrency must be null or a whole number from 2 to 1000."
  }
}

variable "max_receive_count" {
  description = "Receives before a message moves to the DLQ."
  type        = number
  default     = 10
}

variable "function_timeout_seconds" {
  description = "The function's timeout. Queue visibility is six times this, as AWS recommends for Lambda sources."
  type        = number
  default     = 30
}

variable "retention_seconds" {
  description = "Queue and DLQ retention. 14 days, the SQS maximum."
  type        = number
  default     = 1209600
}

variable "delivery_max_retry_attempts" {
  description = "EventBridge retries of a failed put to the queue before the event goes to the delivery DLQ (0-185)."
  type        = number
  default     = 185
  validation {
    condition     = var.delivery_max_retry_attempts >= 0 && var.delivery_max_retry_attempts <= 185
    error_message = "delivery_max_retry_attempts: 0 to 185, the EventBridge range."
  }
}

variable "delivery_max_event_age_seconds" {
  description = "How long EventBridge keeps retrying a failed put to the queue before the delivery DLQ (60-86400)."
  type        = number
  default     = 86400
  validation {
    condition     = var.delivery_max_event_age_seconds >= 60 && var.delivery_max_event_age_seconds <= 86400
    error_message = "delivery_max_event_age_seconds: 60 to 86400, the EventBridge range."
  }
}

variable "age_alarm_threshold_seconds" {
  description = "ApproximateAgeOfOldestMessage threshold for the age alarm."
  type        = number
  default     = 1800
}

variable "alarm_topic_arn" {
  description = "SNS topic every alarm notifies. Null: the alarms exist but notify nobody."
  type        = string
  default     = null
}

variable "create_inbox_table" {
  description = "Create the DynamoDB inbox table (pk/sk strings, TTL on `expires_at`, PITR on, on-demand)."
  type        = bool
  default     = true
}

variable "inbox_table_name" {
  description = "Inbox table name. Null: `<name>-inbox`."
  type        = string
  default     = null
}

variable "role_name" {
  description = "The consumer function's IAM role. Set: `consumer_policy_json` is attached to it as an inline policy. Must be known at plan."
  type        = string
  default     = null
}
