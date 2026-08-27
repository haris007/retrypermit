.PHONY: install web lint test build run

install:
	python3 -m venv .venv
	.venv/bin/pip install -e '.[dev]'

web:
	cd frontend && npm ci && npm run build

lint:
	.venv/bin/ruff check src tests

test:
	.venv/bin/pytest

build: web

run:
	.venv/bin/uvicorn retrypermit.main:app --host 127.0.0.1 --port 8080
