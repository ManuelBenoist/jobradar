# ------ MOT-CLÉS ------
locals {
  job_queries = {
    "data_engineer"    = "Data Engineer"
    "devops"           = "DevOps"
    "data_analyst"     = "Data Analyst"
    "data_ingenieur"   = "Data Ingénieur"
    "cloud_architect"  = "Cloud Architect"
    "ingenieur_devops" = "Ingénieur devops"
  }
  # Mots-clés limités pour JSearch/Jooble (4 au lieu de 6)
  premium_job_queries = {
    "data_engineer"   = "Data Engineer"
    "devops"          = "DevOps"
    "data_analyst"    = "Data Analyst"
    "cloud_architect" = "Cloud Architect"
  }
}
# ------ REGLES DE PLANIFICATION (EventBridge) ------
# une règle par mot-clé (6 règles au total)
resource "aws_cloudwatch_event_rule" "daily_keyword" {
  for_each            = local.job_queries
  name                = "jobradar-daily-${each.key}"
  description         = "Declenche l'ingestion pour ${each.value} à 5h00 UTC (7h00 Paris)"
  schedule_expression = "cron(0 5 * * ? *)"
}
# règle pour les premiums : Lundi, Mercredi, Vendredi à 5h00 UTC
resource "aws_cloudwatch_event_rule" "triweekly_premium_keyword" {
  for_each            = local.premium_job_queries
  name                = "jobradar-premium-${each.key}"
  description         = "Ingestion JSearch/Jooble (MWF) pour ${each.value}"
  schedule_expression = "cron(0 5 ? * MON,WED,FRI *)"
}

# ------ CIBLES (EventBridge) ------
# Cibles Adzuna : on les lie à leur règle respective
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

# Cibles France Travail : on les lie à leur règle respective
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

# Cibles JSearch
resource "aws_cloudwatch_event_target" "target_jsearch" {
  for_each  = local.premium_job_queries
  rule      = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].name
  target_id = "TriggerJSearch-${each.key}"
  arn       = aws_lambda_function.ingest_jsearch.arn
  input     = jsonencode({"keyword": each.value, "where": "Nantes"})
}

# Cibles Jooble
resource "aws_cloudwatch_event_target" "target_jooble" {
  for_each  = local.premium_job_queries
  rule      = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].name
  target_id = "TriggerJooble-${each.key}"
  arn       = aws_lambda_function.ingest_jooble.arn
  input     = jsonencode({"keyword": each.value, "where": "Nantes"})
}

# ------ PERMISSIONS (EventBridge -> Lambda) ------
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

resource "aws_lambda_permission" "allow_jsearch" {
  for_each      = local.premium_job_queries
  statement_id  = "AllowExecutionFromEventBridge-JSearch-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_jsearch.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].arn
}

resource "aws_lambda_permission" "allow_jooble" {
  for_each      = local.premium_job_queries
  statement_id  = "AllowExecutionFromEventBridge-Jooble-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_jooble.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].arn
}