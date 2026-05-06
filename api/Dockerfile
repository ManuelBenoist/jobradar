# Image de base optimisée pour AWS Lambda (Python 3.11)
FROM public.ecr.aws/lambda/python:3.13

# Désactivation de la génération de fichiers .pyc pour optimiser l'espace
ENV PYTHONDONTWRITEBYTECODE=1

# Installation des dépendances
COPY requirements-api.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements-api.txt

# Copie du code source dans le dossier de travail Lambda
COPY . ${LAMBDA_TASK_ROOT}

# Définition du point d'entrée pour Mangum (FastAPI wrapper)
CMD [ "main.handler" ]