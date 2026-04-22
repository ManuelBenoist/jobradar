# 1. Utilisation de l'image de base officielle AWS pour Lambda (Python 3.11)
FROM public.ecr.aws/lambda/python:3.11

# 2. Variable d'environnement pour que Python ne crée pas de fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE=1

# 3. Copier le fichier des dépendances
COPY requirements-api.txt ${LAMBDA_TASK_ROOT}

# 4. Installation des dépendances directement dans l'environnement de la Lambda
RUN pip install --no-cache-dir -r requirements-api.txt

# 5. Copier le code source de l'API
COPY . ${LAMBDA_TASK_ROOT}

# 6. Le point d'entrée (HANDLER)
CMD [ "main.handler" ]