variable "aws_region" {
  default     = "eu-west-3"
  description = "Région AWS où les ressources seront créées"
}

variable "project_name" {
  default     = "jobradar"
  description = "Nom du projet utilisé pour nommer les ressources"
}

# Variables pour les credentials de l'API Adzuna
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

# Variables pour les credentials de l'API France Travail
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
