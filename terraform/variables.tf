variable "aws_region" {
  default     = "eu-west-3"
  description = "Région AWS où les ressources seront créées"
}

variable "project_name" {
  default     = "jobradar"
  description = "Nom du projet utilisé pour nommer les ressources"
}

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
