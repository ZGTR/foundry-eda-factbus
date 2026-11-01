variable "prefix" {
  description = "The product's short name. It prefixes every resource name and the event source (`<prefix>.orders`)."
  type        = string
  default     = "shop"
  validation {
    condition     = can(regex("^[a-z0-9-]{1,30}$", var.prefix))
    error_message = "prefix: 1 to 30 lowercase letters, digits or '-'."
  }
}

variable "localstack_endpoint" {
  description = "LocalStack edge URL, e.g. http://localhost:4576. Null: the provider talks to real AWS with the caller's own credentials and region."
  type        = string
  default     = null
}

variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "sweeper_function_arn" {
  description = "The Lambda that runs `SqlProducer.sweep_handler()`. Set: the producer module schedules it every minute. Null: no schedule (the e2e publishes after commit and never needs the sweep)."
  type        = string
  default     = null
}
