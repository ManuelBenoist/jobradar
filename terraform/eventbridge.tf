locals {
  job_queries = {
    "data_engineer"    = "Data Engineer"
    "devops"           = "DevOps"
    "data_analyst"     = "Data Analyst"
    "data_ingenieur"   = "Data Ingénieur"
    "cloud_architect"  = "Cloud Architect"
    "ingenieur_devops" = "Ingénieur devops"
  }
}

# 1. On crée UNE RÈGLE par mot-clé (6 règles au total)
resource "aws_cloudwatch_event_rule" "daily_keyword" {
  for_each            = local.job_queries
  name                = "jobradar-daily-${each.key}"
  description         = "Declenche l'ingestion pour ${each.value} à 8h00 UTC"
  schedule_expression = "cron(0 8 * * ? *)"
}

# 2. Cibles Adzuna : on les lie à leur règle respective
resource "aws_cloudwatch_event_target" "target_adzuna" {
  for_each  = local.job_queries
  rule      = aws_cloudwatch_event_rule.daily_keyword[each.key].name
  target_id = "TriggerAdzuna-${each.key}"
  arn       = aws_lambda_function.ingest_adzuna.arn
  
  input = jsonencode({
    "keyword": each.value,
    "where": "Nantes"
  })
}

# 3. Cibles France Travail : on les lie à leur règle respective
resource "aws_cloudwatch_event_target" "target_france_travail" {
  for_each  = local.job_queries
  rule      = aws_cloudwatch_event_rule.daily_keyword[each.key].name
  target_id = "TriggerFT-${each.key}"
  arn       = aws_lambda_function.ingest_france_travail.arn

  input = jsonencode({
    "keyword": each.value,
    "departement": 44
  })
}

# 4. Permissions : On autorise TOUTES les nouvelles règles à appeler les Lambdas
resource "aws_lambda_permission" "allow_eventbridge_adzuna" {
  for_each      = local.job_queries
  statement_id  = "AllowExecutionFromCloudWatch-Adzuna-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_adzuna.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_keyword[each.key].arn
}

resource "aws_lambda_permission" "allow_eventbridge_ft" {
  for_each      = local.job_queries
  statement_id  = "AllowExecutionFromCloudWatch-FT-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_france_travail.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_keyword[each.key].arn
}
