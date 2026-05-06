# ============================================================================
# ORCHESTRATION : EVENTBRIDGE RULES & TARGETS
# ============================================================================

locals {
  # Mots-clés pour l'ingestion quotidienne (Standard : Adzuna & France Travail)
  job_queries = {
    "data_engineer"    = "Data Engineer"
    "devops"           = "DevOps"
    "data_analyst"     = "Data Analyst"
    "data_ingenieur"   = "Data Ingénieur"
    "cloud_architect"  = "Cloud Architect"
    "ingenieur_devops" = "Ingénieur devops"
  }

  # Mots-clés pour l'ingestion tri-hebdomadaire (Premium : JSearch & Jooble)
  # Limitation pour optimiser les quotas d'API et les coûts de calcul.
  premium_job_queries = {
    "data_engineer"   = "Data Engineer"
    "devops"          = "DevOps"
    "data_analyst"    = "Data Analyst"
    "cloud_architect" = "Cloud Architect"
  }
}

# ------ RÈGLES DE PLANIFICATION ------

resource "aws_cloudwatch_event_rule" "daily_keyword" {
  for_each            = local.job_queries
  name                = "jobradar-daily-${each.key}"
  description         = "Trigger quotidien pour ${each.value} à 05:00 UTC"
  schedule_expression = "cron(0 5 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "triweekly_premium_keyword" {
  for_each            = local.premium_job_queries
  name                = "jobradar-premium-${each.key}"
  description         = "Trigger Premium (Lundi, Mercredi, Vendredi) pour ${each.value}"
  schedule_expression = "cron(0 5 ? * MON,WED,FRI *)"
}

# ------ CIBLES DES LAMBDAS ------

resource "aws_cloudwatch_event_target" "target_adzuna" {
  for_each  = local.job_queries
  rule      = aws_cloudwatch_event_rule.daily_keyword[each.key].name
  target_id = "TriggerAdzuna-${each.key}"
  arn       = aws_lambda_function.ingest_adzuna.arn
  input     = jsonencode({ "keyword" : each.value, "where" : "Nantes" })
}

resource "aws_cloudwatch_event_target" "target_france_travail" {
  for_each  = local.job_queries
  rule      = aws_cloudwatch_event_rule.daily_keyword[each.key].name
  target_id = "TriggerFT-${each.key}"
  arn       = aws_lambda_function.ingest_france_travail.arn
  input     = jsonencode({ "keyword" : each.value, "departement" : 44 })
}

resource "aws_cloudwatch_event_target" "target_jsearch" {
  for_each  = local.premium_job_queries
  rule      = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].name
  target_id = "TriggerJSearch-${each.key}"
  arn       = aws_lambda_function.ingest_jsearch.arn
  input     = jsonencode({ "keyword" : each.value, "where" : "Nantes" })
}

resource "aws_cloudwatch_event_target" "target_jooble" {
  for_each  = local.premium_job_queries
  rule      = aws_cloudwatch_event_rule.triweekly_premium_keyword[each.key].name
  target_id = "TriggerJooble-${each.key}"
  arn       = aws_lambda_function.ingest_jooble.arn
  input     = jsonencode({ "keyword" : each.value, "where" : "Nantes" })
}

# ------ PERMISSIONS D'INVOCATION (Security) ------

resource "aws_lambda_permission" "allow_eventbridge" {
  for_each = merge(
    { for k, v in local.job_queries : "adzuna-${k}" => { arn = aws_lambda_function.ingest_adzuna.function_name, rule = aws_cloudwatch_event_rule.daily_keyword[k].arn } },
    { for k, v in local.job_queries : "ft-${k}" => { arn = aws_lambda_function.ingest_france_travail.function_name, rule = aws_cloudwatch_event_rule.daily_keyword[k].arn } },
    { for k, v in local.premium_job_queries : "jsearch-${k}" => { arn = aws_lambda_function.ingest_jsearch.function_name, rule = aws_cloudwatch_event_rule.triweekly_premium_keyword[k].arn } },
    { for k, v in local.premium_job_queries : "jooble-${k}" => { arn = aws_lambda_function.ingest_jooble.function_name, rule = aws_cloudwatch_event_rule.triweekly_premium_keyword[k].arn } }
  )
  statement_id  = "AllowExecutionFromEventBridge-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = each.value.arn
  principal     = "events.amazonaws.com"
  source_arn    = each.value.rule
}