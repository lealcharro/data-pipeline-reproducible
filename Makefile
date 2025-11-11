.PHONY: help setup build run test clean verify-hash run-all

help:
	@echo "Comandos disponibles:"
	@echo "  make setup       - Configurar ambiente inicial"
	@echo "  make build       - Construir contenedores Docker"
	@echo "  make run         - Ejecutar pipeline completo"
	@echo "  make test        - Ejecutar todos los tests"
	@echo "  make clean       - Limpiar datos y cache"
	@echo "  make verify-hash - Verificar reproducibilidad"

setup:
	pip install -r requirements.txt
	mkdir -p data/{input,intermediate,output}
	mkdir -p tests/fixtures

build:
	docker-compose build

run:
	docker-compose up

test:
	pytest tests/ -v --cov=pipeline --cov-report=html

clean:
	rm -rf data/intermediate/* data/output/*
	find . -type d -name __pycache__ -exec rm -r {} +
	docker-compose down -v

verify-hash:
	python scripts/verify_reproducibility.py

run-all: clean build run verify-hash
