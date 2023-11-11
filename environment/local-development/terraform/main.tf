variable "default_endpoint" {
  description = "Endpoint padrão para os serviços AWS local."
  default     = "http://localhost:4566"
  type        = string
}


provider "aws" {
  access_key = "FAKE"
  secret_key = "FAKE"
  region     = "us-east-1"

  # only required for non virtual hosted-style endpoint use case.
  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs#s3_use_path_style
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    #    apigateway = var.default_endpoint
    #    dynamodbstreams = var.default_endpoint
    #    ioteventsdata = var.default_endpoint
    #    iotwireless = var.default_endpoint
    #    mediastoredata = var.default_endpoint
    #    qldbsession = var.default_endpoint
    #    rdsdata = var.default_endpoint
    #    sagemakerruntime = var.default_endpoint
    #    support = var.default_endpoint
    #    timestreamquery = var.default_endpoint
    cloudwatchlogs = var.default_endpoint
    cloudwatch     = var.default_endpoint
    dynamodb       = var.default_endpoint
    iam            = var.default_endpoint
    lambda         = var.default_endpoint
    sqs            = var.default_endpoint
    ec2            = var.default_endpoint
    s3             = var.default_endpoint
    kms            = var.default_endpoint
    firehose       = var.default_endpoint
    secretsmanager = var.default_endpoint
  }
}

resource "aws_secretsmanager_secret" "localstack" {
  name = "localstack"
}

terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "0.5.3"
    }
  }
}

#provider "kafka" {
#  bootstrap_servers = ["localhost:9093"]
#  skip_tls_verify   = true
#}
#
#resource "kafka_topic" "data-ingestion-pipeline-" {
#  name               = "data-ingestion-pipeline-poc_stream"
#  replication_factor = 1
#  partitions         = 1
#  #  config = {
#  #    "segment.ms"     = "20000"
#  #    "cleanup.policy" = "compact"
#  #  }
#}

resource "aws_dynamodb_table" "poc_stream" {
  name             = "poc_stream"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "id"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
  ttl {
    attribute_name = "timeToExist"
    enabled        = true
  }
  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_iam_role" "dynamodb_assume_role" {
  name               = "dynamodb_assume_role"
  path               = "/service-role/"
  assume_role_policy = jsonencode({
    Version   = "2021-01-01"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

data "aws_iam_policy_document" "use_case_capture_dynamodbstream" {
  statement {
    effect  = "Allow"
    actions = [
      "lambda:InvokeFunction",
    ]
    resources = [
      "${aws_lambda_function.lambda_capture.arn}"
    ]
  }

  statement {
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "${aws_lambda_function.lambda_capture.arn}",
      "${aws_dynamodb_table.poc_stream.stream_arn}"
    ]
  }

  statement {
    effect  = "Allow"
    actions = [
      "dynamodb:DescribeStream",
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:ListStreams"
    ]
    resources = [
      "${aws_dynamodb_table.poc_stream.stream_arn}"
    ]
  }
}


resource "aws_iam_role_policy" "dynamodb_role_policy" {
  name       = "dynamodb_role_policy"
  role       = aws_iam_role.dynamodb_assume_role.id
  depends_on = [
    aws_dynamodb_table.poc_stream,
    aws_lambda_function.lambda_capture
  ]
  policy = data.aws_iam_policy_document.use_case_capture_dynamodbstream.json
}

resource "null_resource" "install" {
  triggers = {
    shell_hash = sha256(timestamp())
  }

  provisioner "local-exec" {
    command = "ls -lah ../../../"
    # command = "rm -rf ../capture_lambda/target && pip install --target ../capture_lambda/target -r ../capture_lambda/requirements.txt && cp -r ../capture_lambda/src/* ../capture_lambda/target"
  }
}

data "archive_file" "capture_lambda_zip" {
  type        = "zip"
  source_dir  = "../../../target/"
  output_path = "../../../dist/dataplatform_dynamodb_generic_capture_trigger.zip"
  depends_on  = [null_resource.install]
}

resource "aws_lambda_layer_version" "capture_lambda" {
  layer_name          = "capture_lambda"
  filename            = data.archive_file.capture_lambda_zip.output_path
  source_code_hash    = data.archive_file.capture_lambda_zip.output_base64sha256
  compatible_runtimes = ["python3.8"]
}


resource "aws_lambda_function" "lambda_capture" {
  function_name    = "dataplatform_dynamodb_generic_capture_trigger"
  runtime          = "python3.8"
  handler          = "send_to_kafka_function.lambda_handler"
  filename         = data.archive_file.capture_lambda_zip.output_path
  source_code_hash = data.archive_file.capture_lambda_zip.output_base64sha256
  role             = aws_iam_role.dynamodb_assume_role.arn
  layers           = [aws_lambda_layer_version.capture_lambda.arn]
  depends_on       = [
    aws_iam_role.dynamodb_assume_role
  ]
  environment {
    variables = {
      "KAFKA_HOSTS" : "kafka:9091"
    }
  }

}


resource "aws_lambda_event_source_mapping" "trigger" {
  event_source_arn  = aws_dynamodb_table.poc_stream.stream_arn
  function_name     = aws_lambda_function.lambda_capture.arn
  batch_size        = 10
  starting_position = "LATEST"
  # starting_position = "TRIM_HORIZON"
  depends_on        = [
    aws_dynamodb_table.poc_stream,
    aws_lambda_function.lambda_capture
  ]
}


output "dynamodb_table_id" {
  description = "aws_dynamodb_table.poc_stream.id"
  value       = aws_dynamodb_table.poc_stream.id
}


output "dynamodb_table_arn" {
  description = "aws_dynamodb_table.poc_stream.arn"
  value       = aws_dynamodb_table.poc_stream.arn
}


output "dynamodb_table_stream_arn" {
  description = "aws_dynamodb_table.poc_stream.stream_arn"
  value       = aws_dynamodb_table.poc_stream.stream_arn
}


output "lambda_function_capture_arn" {
  description = "aws_lambda_function.lambda_capture.arn"
  value       = aws_lambda_function.lambda_capture.arn
}