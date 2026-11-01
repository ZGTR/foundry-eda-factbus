# A complete product root: the pipe (one bus), the producer (PutEvents under the
# product's own source) and one consumer (rule -> queue + DLQs -> Lambda, with its inbox).
# A product copies this directory, renames `shop` and `order.placed`, swaps the relative
# module sources for the git sources in README.md, and keeps only what is its own:
# its fact types, its handler and its module calls.
#
#   LocalStack:  tofu apply -var localstack_endpoint=http://localhost:4576
#   Real AWS:    tofu apply   (no endpoint; your credentials and region decide)
#
# No notifications module here: that consumer is optional and lives in
# ZGTR/mshaker-foundry-eda-notifications.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

locals {
  local        = var.localstack_endpoint != null
  event_source = "${var.prefix}.orders"
  event_type   = "order.placed"
  # The directory that holds the `factbus/` package the Lambda ships. Here it is this
  # repository's src/; a product points `factbus_src_dir` at its vendored copy.
  src_dir = coalesce(var.factbus_src_dir, "${path.module}/../../src")
}

provider "aws" {
  region = var.region

  # LocalStack accepts any key pair; real AWS uses the caller's credential chain.
  access_key                  = local.local ? "test" : null
  secret_key                  = local.local ? "test" : null
  skip_credentials_validation = local.local
  skip_metadata_api_check     = local.local
  skip_requesting_account_id  = local.local

  dynamic "endpoints" {
    for_each = local.local ? [var.localstack_endpoint] : []
    content {
      cloudwatch = endpoints.value
      dynamodb   = endpoints.value
      events     = endpoints.value
      iam        = endpoints.value
      lambda     = endpoints.value
      logs       = endpoints.value
      sqs        = endpoints.value
      sts        = endpoints.value
    }
  }
}

# --- pipe ------------------------------------------------------------------------------

module "bus" {
  source = "../../iac/modules/bus"
  name   = "${var.prefix}-events"
}

# --- producer --------------------------------------------------------------------------

# The role the product's application runs as. A real product already has one (its API's
# Lambda or task role); pass that role's name to the producer module instead.
resource "aws_iam_role" "app" {
  name = "${var.prefix}-app"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# PutEvents on this bus under the product's own source, attached to the app role. Set
# `sweeper_function_arn` to the Lambda that runs `SqlProducer.sweep_handler()` and the
# module schedules it every minute. Skip this module when the product's own stack already
# grants PutEvents and schedules the sweeper (Shepherd and Alqosh do).
module "producer" {
  source               = "../../iac/modules/producer"
  bus_arn              = module.bus.bus_arn
  event_source         = local.event_source
  role_name            = aws_iam_role.app.name
  sweeper_function_arn = var.sweeper_function_arn
}

# --- one consumer ----------------------------------------------------------------------

# The consumer's own state: one item per applied order. A second copy of the same event
# would show up as a second item, so the e2e can count applies.
resource "aws_dynamodb_table" "ledger" {
  name         = "${var.prefix}-order-ledger"
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
}

# handler.py plus the factbus package from `local.src_dir`. boto3 ships with the
# python3.12 Lambda runtime. A product with its own build (serverless, SAM, a container)
# replaces this archive with that build's artifact.
data "archive_file" "consumer" {
  type        = "zip"
  output_path = "${path.module}/.build/consumer.zip"

  source {
    content  = file("${path.module}/handler.py")
    filename = "handler.py"
  }

  dynamic "source" {
    for_each = fileset(local.src_dir, "factbus/**/*.py")
    content {
      content  = file("${local.src_dir}/${source.value}")
      filename = source.value
    }
  }
}

resource "aws_iam_role" "consumer" {
  name = "${var.prefix}-order-ledger"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# The handler's own writes and logs. The queue and inbox grants come from the consumer
# module (`role_name` below).
resource "aws_iam_role_policy" "consumer" {
  name = "${var.prefix}-order-ledger-own"
  role = aws_iam_role.consumer.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteLedger"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = aws_dynamodb_table.ledger.arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.consumer.arn}:*"
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${var.prefix}-order-ledger"
  retention_in_days = 14
}

resource "aws_lambda_function" "consumer" {
  function_name    = "${var.prefix}-order-ledger"
  role             = aws_iam_role.consumer.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.consumer.output_path
  source_code_hash = data.archive_file.consumer.output_base64sha256
  timeout          = 30
  depends_on       = [aws_cloudwatch_log_group.consumer]

  environment {
    variables = {
      FACTBUS_INBOX_TABLE = "${var.prefix}-order-ledger-inbox"
      LEDGER_TABLE        = aws_dynamodb_table.ledger.name
      CONSUMER_SERVICE    = "${var.prefix}-order-ledger"
    }
  }
}

# The whole consumer in one block: rule on the bus, queue, DLQ, delivery DLQ, event
# source mapping, alarms and the DynamoDB inbox.
module "consumer_order_ledger" {
  source       = "../../iac/modules/consumer"
  name         = "${var.prefix}-order-ledger"
  bus_name     = module.bus.bus_name
  sources      = [local.event_source]
  event_types  = [local.event_type]
  function_arn = aws_lambda_function.consumer.arn
  role_name    = aws_iam_role.consumer.name
  # Named here rather than read back from the module, which would make the function
  # depend on the module that depends on the function.
  inbox_table_name = "${var.prefix}-order-ledger-inbox"
}
