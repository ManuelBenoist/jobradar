# Variables
PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

# Couleurs
HELP_COLOR = \033[36m
RESET = \033[0m

.PHONY: help setup terraform-plan dbt-run ui clean

help:
	@echo "$(HELP_COLOR)JobRadar - Modern Data Stack Control$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(HELP_COLOR)%-15s$(RESET) %s\n", $$1, $$2}'

setup:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt
	@echo "✅ Setup terminé. Activez avec: source .venv/bin/activate"

terraform-init:
	cd terraform && terraform init && terraform fmt && terraform validate

dbt-setup:
	cd transform && $(BIN)/dbt deps && $(BIN)/dbt seed

dbt-run:
	cd transform && $(BIN)/dbt run

spark-transform:
	$(BIN)/python src/spark/transform.py

ui:
	$(BIN)/streamlit run ui/app.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache transform/target transform/logs
	@echo "✨ Workspace nettoyé."
