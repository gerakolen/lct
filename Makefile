ENVIRONMENT ?= test
LCT_URL=http://127.0.0.1:8998
JSON_FILE=info/sample_request.json
TASK_ID ?= 0b887633b67c43799325b9ce542a4d07

.PHONY: help

# Help section
help:
	@echo "Makefile for operations"
	@echo
	@echo "Usage:"
	@echo "  make <target> [ENVIRONMENT=<environment>]"
	@echo
	@echo "Targets:"
	@echo "  help         - Display this help message"
	@echo "  install      - Install command"
	@echo "  more_cmd     - Other commands should go here"
	@echo
	@echo "Variables:"
	@echo "  ENVIRONMENT - Specify the environment (default: test)"
	@echo
	@echo "Example:"
	@echo "  make install ENVIRONMENT=prod"

.PHONY: start
start:
	 uvicorn app.main:app --reload


.PHONY: redis_up
redis_up:
	 docker run -p 6379:6379 redis

.PHONY: celery_worker
celery_worker:
	 celery -A app.task worker --loglevel=info


.PHONY: new_rq
new_rq:
	curl -X POST $(LCT_URL)/new \
		-H "Content-Type: application/json" \
		-d @$(JSON_FILE)

.PHONY: status
status:
	curl $(LCT_URL)/status?task_id=$(TASK_ID)

.PHONY: getresult
getresult:
	curl $(LCT_URL)/getresult?task_id=$(TASK_ID)


# uv helper commands
.PHONY: uv_install
uv_install:
	@uv sync --dev

.PHONY: uv_requirements
uv_requirements:
	@uv export --no-dev --no-emit-project --no-hashes --no-header > requirements.txt

.PHONY: uv_test
uv_test:
	@pytest --cov-report xml:coverage-reports/coverage-report.xml --cov=lct ./tests/ --junitxml=python-test-report.xml

.PHONY: lint
lint:
	@ruff check --fix
	@ruff format
	@ty check
