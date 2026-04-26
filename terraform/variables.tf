# Variables pour la configuration AWS
variable "aws_region" {
  default     = "eu-west-3"
  description = "Région AWS où les ressources seront créées"
}

variable "project_name" {
  default     = "jobradar"
  description = "Nom du projet utilisé pour nommer les ressources"
}

# API Adzuna
variable "adzuna_app_id" {
  description = "ID de l'application Adzuna"
  type        = string
  sensitive   = true 
}

variable "adzuna_app_key" {
  description = "Clé secrète de l'application Adzuna"
  type        = string
  sensitive   = true
}

# API France Travail
variable "ft_client_id" {
  description = "Client ID pour l'API France Travail"
  type        = string
  sensitive   = true
}

variable "ft_client_secret" {
  description = "Client Secret pour l'API France Travail"
  type        = string
  sensitive   = true
}

# API JSearch (RapidAPI)
variable "jsearch_api_key" {
  description = "Clé API pour JSearch de RapidAPI"
  type        = string
  sensitive   = true
}

# API Jooble
variable "jooble_api_key" {
  description = "Clé API pour Jooble"
  type        = string
  sensitive   = true
}

# Variable pour le bucket de staging Athena
variable "athena_staging_dir" {
  description = "Le bucket S3 pour les résultats Athena"
  type        = string
}