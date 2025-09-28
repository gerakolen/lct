ENVIRONMENT ?= test
LCT_URL=http://127.0.0.1:8998

SAMPLE_REQUEST_FILE=info/request/sample.json
EXTENDED_REQUEST_FILE=info/request/extended.json
INVALID_REQUEST_FILE=info/request/invalid_format.json
SQL_EXPLAIN_FILE=info/request/sql_explain.json
QUESTH_REQUEST_FILE=info/request/questsH.json

TASK_ID ?= 9d8edbee-5f4a-4259-bd5e-151dfa9d7742

USERNAME?=user
PASSWORD?=password

GIT_HASH := $(shell git rev-parse --short HEAD)
IMAGE_NAME := gerakolen/lct:1.0.0-$(GIT_HASH)

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


############## APP START COMMANDS ##############

.PHONY: redis_start
redis_start:
	 docker run -p 6379:6379 redis

.PHONY: app_start
app_start:
	 uvicorn app.main:app --port 8998 --log-level info --reload

.PHONY: celery_worker_start
celery_worker_start:
	 celery -A app.task worker --loglevel=info

.PHONY: dbuild
dbuild:
	 docker build -f Dockerfile . --build-arg GIT_HASH=$(GIT_HASH) -t $(IMAGE_NAME)

.PHONY: dcup
dcup:
	 docker compose up


############## API TESTING COMMANDS ##############
.PHONY: new_rq
new_rq:
	curl -u $(USERNAME):$(PASSWORD) \
	    -X POST $(LCT_URL)/new \
		-H "Content-Type: application/json" \
		-d @$(EXTENDED_REQUEST_FILE)

.PHONY: new_questsH
new_questsH:
	curl -u $(USERNAME):$(PASSWORD) \
	    -X POST $(LCT_URL)/new \
		-H "Content-Type: application/json" \
		-d @$(QUESTH_REQUEST_FILE)


.PHONY: new_invalid_rq
new_invalid_rq:
	curl -vvv -u $(USERNAME):$(PASSWORD) \
	    -X POST $(LCT_URL)/new \
		-H "Content-Type: application/json" \
		-d @$(INVALID_REQUEST_FILE)

.PHONY: poll_status
poll_status:
	@while true; do \
		status=$$(curl -s -u $(USERNAME):$(PASSWORD) "$(LCT_URL)/status?task_id=$(TASK_ID)" | jq -r .status); \
		echo "Status: $$status"; \
		if [ "$$status" = "COMPLETE" ]; then \
			echo "Task is COMPLETE!"; \
			break; \
		fi; \
		sleep 1; \
	done

.PHONY: getresult
getresult:
	curl -u $(USERNAME):$(PASSWORD) $(LCT_URL)/getresult?task_id=$(TASK_ID)

.PHONY: explain
explain:
	curl -u $(USERNAME):$(PASSWORD) \
	    -X POST $(LCT_URL)/explain \
		-H "Content-Type: application/json" \
		-d @$(SQL_EXPLAIN_FILE)

############## TESTING UTILS ##############

.PHONY: gen_input
gen_input:
	python scripts/generate_input_json.py --ddl 3 --queries 5


############## UV HELPER COMMANDS ##############
.PHONY: uv_install
uv_install:
	@uv sync --dev

.PHONY: uv_requirements
uv_requirements:
	@uv export --no-dev --no-emit-project --no-hashes --no-header > requirements.txt

.PHONY: uv_test
uv_test:
	uv run -m pytest --cov-report=html --cov=app ./tests/app

.PHONY: lint
lint:
	@ruff check --fix
	@ruff format
	@ty check
