# Variables
PYTHON = python3
VENV = .venv
PIP = $(VENV)/bin/pip
PY_BIN = $(VENV)/bin/python

# Couleurs pour le terminal
HELP_COLOR = \033[36m
RESET = \033[0m

.PHONY: help setup scan ingest-raw load-duckdb clean lint

help: ## Affiche cette aide
	@echo "$(HELP_COLOR)JobRadar Nantes - Automation Menu$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(HELP_COLOR)%-15s$(RESET) %s\n", $$1, $$2}'

setup: ## Initialise l'environnement virtuel et installe les dépendances
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✅ Environnement prêt. Activez-le avec: source .venv/bin/activate"

scan: ## Lance un audit de sécurité avec Gitleaks
	gitleaks detect --source . -v

lint: ## Vérifie la qualité du code (PEP8)
	$(PY_BIN) -m flake8 ingestion/ transform/

ingest-raw: ## Étape 1: Récupère les offres Adzuna et France Travail (JSON)
	$(PY_BIN) src/ingestion/fetch_adzuna.py
	$(PY_BIN) src/ingestion/fetch_france_travail.py

load-duckdb: ## Étape 2: Charge les JSON dans DuckDB (Issue #4)
	$(PY_BIN) src/ingestion/load_to_duckdb.py

clean: ## Nettoie les fichiers temporaires et les logs
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf ingestion/__pycache__
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "✨ Nettoyage terminé."

reset-data: ## Supprime la base DuckDB et les données raw pour repartir à zéro
	rm -f data/raw/*.json
	rm -f jobradar.duckdb
	@echo "🗑️  Données supprimées."