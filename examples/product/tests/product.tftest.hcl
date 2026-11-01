# examples/product, offline: mock providers and plan-only runs. Proves the three roles
# are wired to each other (the producer may publish only to this bus under the product's
# source; the consumer listens on this bus for that source) and that the example carries
# no notifications module. No AWS credentials and no AWS call. The LocalStack run in
# scripts/e2e_product.py proves the same wiring by publishing and consuming one fact.

# Mocked ARNs must parse as ARNs: the provider validates them even at plan.
mock_provider "aws" {
  mock_resource "aws_sqs_queue" {
    defaults = {
      arn = "arn:aws:sqs:us-east-1:111111111111:shop-order-ledger"
      id  = "https://sqs.us-east-1.amazonaws.com/111111111111/shop-order-ledger"
    }
  }
  mock_resource "aws_cloudwatch_event_rule" {
    defaults = { arn = "arn:aws:events:us-east-1:111111111111:rule/shop-events/shop-order-ledger" }
  }
  mock_resource "aws_lambda_function" {
    defaults = { arn = "arn:aws:lambda:us-east-1:111111111111:function:shop-order-ledger" }
  }
  mock_resource "aws_dynamodb_table" {
    defaults = { arn = "arn:aws:dynamodb:us-east-1:111111111111:table/shop-order-ledger" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::111111111111:role/shop-app" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:us-east-1:111111111111:log-group:/aws/lambda/shop-order-ledger" }
  }
}
mock_provider "archive" {}

override_module {
  target = module.bus
  outputs = {
    bus_name    = "shop-events"
    bus_arn     = "arn:aws:events:us-east-1:111111111111:event-bus/shop-events"
    archive_arn = "arn:aws:events:us-east-1:111111111111:archive/shop-events-archive"
  }
}

run "producer_publishes_only_to_this_bus_under_the_products_source" {
  command = plan
  assert {
    condition = {
      for s in jsondecode(output.producer_policy_json).Statement : s.Sid => [s.Resource, try(s.Condition.StringEquals["events:source"], null)]
      } == {
      PublishAsOwnSource = ["arn:aws:events:us-east-1:111111111111:event-bus/shop-events", "shop.orders"]
      DescribeBus        = ["arn:aws:events:us-east-1:111111111111:event-bus/shop-events", null]
    }
    error_message = "the producer policy must name this bus and the product's source: ${output.producer_policy_json}"
  }
  assert {
    condition     = output.event_source == "shop.orders" && output.event_type == "order.placed"
    error_message = "the fact is order.placed from shop.orders"
  }
}

run "prefix_renames_the_source_and_every_name" {
  command = plan
  variables {
    prefix = "acme"
  }
  assert {
    condition     = output.event_source == "acme.orders" && strcontains(output.producer_policy_json, "\"acme.orders\"")
    error_message = "prefix must rename the event source the producer is held to"
  }
}

# A root cannot read a child module's resources from a test, so the consumer's wiring is
# read from the call itself: it must take the bus from module.bus and its source from the
# same local the producer is held to.
run "consumer_is_wired_to_the_bus_and_the_producers_source" {
  command = plan
  assert {
    condition = alltrue([
      for pattern in [
        "(?m)^\\s*bus_name\\s*=\\s*module\\.bus\\.bus_name\\s*$",
        "(?m)^\\s*sources\\s*=\\s*\\[local\\.event_source\\]\\s*$",
        "(?m)^\\s*event_types\\s*=\\s*\\[local\\.event_type\\]\\s*$",
      ] :
      length(regexall(pattern, one(regex("(?s)module \"consumer_order_ledger\" \\{(.*?)\\n\\}", file("${path.module}/main.tf"))))) == 1
    ])
    error_message = "the consumer must read bus_name from module.bus and sources/event_types from the producer's locals"
  }
  assert {
    condition     = length(regexall("source\\s*=\\s*\"[^\"]*eda-notifications", file("${path.module}/main.tf"))) == 0
    error_message = "the example must not call the notifications module (it is an optional consumer)"
  }
  assert {
    condition     = length(regexall("(?m)^module \"", file("${path.module}/main.tf"))) == 3
    error_message = "positive control: the scan must see the three module calls (bus, producer, consumer)"
  }
}

run "an_empty_prefix_is_refused" {
  command = plan
  variables {
    prefix = ""
  }
  expect_failures = [var.prefix]
}
