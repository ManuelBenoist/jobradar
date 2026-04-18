# --- STAGE 1 : Builder ---
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installation des outils de build
RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev

# Création d'un Virtualenv pour isoler les libs
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Utilisation du nom exact de ton fichier
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt


# --- STAGE 2 : Runtime ---
FROM python:3.11-slim

WORKDIR /app

# On copie TOUT l'environnement virtuel (plus fiable que .local)
COPY --from=builder /opt/venv /opt/venv

# On force l'utilisation des binaires du venv
ENV PATH="/opt/venv/bin:$PATH"

# On copie le reste du code (main.py, etc.)
COPY . .

EXPOSE 8000

# Lancement de l'API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]