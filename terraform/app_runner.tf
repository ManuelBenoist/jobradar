# 1. Rôle IAM pour qu'App Runner puisse piocher dans ECR
resource "aws_iam_role" "apprunner_service_role" {
  name = "apprunner-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "build.apprunner.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "apprunner_ecr_access" {
  role       = aws_iam_role.apprunner_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

# 2. Le service App Runner lui-même
resource "aws_apprunner_service" "jobradar_api" {
  service_name = "jobradar-api-service"

  source_configuration {
    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_service_role.arn
    }
    image_repository {
      image_identifier      = "${aws_ecr_repository.jobradar_api.repository_url}:latest"
      image_repository_type = "ECR"
      image_configuration {
        port = "8000" # Le port exposé du Dockerfile
      }
    }
    auto_deployments_enabled = true # Déploie dès que l'image ECR change
  }

  tags = {
    Name = "jobradar-api"
  }
}

# Afficher l'URL de l'API à la fin du apply
output "app_runner_url" {
  value = aws_apprunner_service.jobradar_api.service_url
}