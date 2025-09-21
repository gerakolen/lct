ENVIRONMENT ?= test
LCT_URL=http://127.0.0.1:8998
JSON_FILE=info/sample_request.json
TASK_ID ?= d45e7618-0424-466e-9980-e74c5042e777

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

.PHONY: aaa
aaa:
	 pwd

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
