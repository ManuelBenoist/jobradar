# ============================================================================
# OBSERVABILITY : CLOUDWATCH LOG RETENTION & METRIC ALARMS
# ============================================================================

locals {
  ingestion_lambdas = {
    adzuna         = aws_lambda_function.ingest_adzuna.function_name
    france_travail = aws_lambda_function.ingest_france_travail.function_name
    jsearch        = aws_lambda_function.ingest_jsearch.function_name
    jooble         = aws_lambda_function.ingest_jooble.function_name
  }
  api_lambdas = {
    api = aws_lambda_function.jobradar_api.function_name
  }
  all_lambdas = merge(local.ingestion_lambdas, local.api_lambdas)
}

# --- LOG GROUP RETENTION (FinOps : éviter l'accumulation infinie) ---

resource "aws_cloudwatch_log_group" "lambda_logs" {
  for_each          = local.all_lambdas
  name              = "/aws/lambda/${each.value}"
  retention_in_days = 14
}

# --- ALARMES : ERREURS LAMBDA ---

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.all_lambdas
  alarm_name          = "jobradar-${each.key}-errors"
  alarm_description   = "Alarme si la Lambda ${each.value} émet des erreurs"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = 300
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = each.value
  }
  tags = {
    Name = "jobradar-${each.key}-errors"
  }
}

# --- ALARMES : THROTTLES LAMBDA ---

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each            = local.all_lambdas
  alarm_name          = "jobradar-${each.key}-throttles"
  alarm_description   = "Alarme si la Lambda ${each.value} est limitée (throttling)"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = 300
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = each.value
  }
  tags = {
    Name = "jobradar-${each.key}-throttles"
  }
}
