# factbus developer targets.
#
#   make test          unit suite (no engines)
#   make up            start compose (LocalStack 3.8, PostgreSQL 16, MySQL 8.0)
#   make integration   integration suite against compose
#   make e2e           apply examples/hello to LocalStack, run scripts/e2e_hello.py, destroy
#   make e2e-product   apply examples/product to LocalStack, run scripts/e2e_product.py, destroy
#   make down          destroy the examples/hello stack (compose stays up)
#   make down-product  destroy the examples/product stack
#   make ci            every gate a pull request must pass, locally (needs compose and tofu)
#
# KEEP=1 make e2e leaves the stack applied; `make down` removes it later.

PY                  ?= .venv/bin/python
LOCALSTACK_ENDPOINT ?= http://localhost:4576
FACTBUS_PG_URL        ?= postgresql+psycopg://factbus:factbus@localhost:5452/factbus
FACTBUS_MYSQL_URL     ?= mysql+pymysql://factbus:factbus@localhost:3326/factbus
HELLO               := examples/hello
HELLO_VARS          := -var localstack_endpoint=$(LOCALSTACK_ENDPOINT) -var test_backoff_factor=0.01
PRODUCT             := examples/product
PRODUCT_VARS        := -var localstack_endpoint=$(LOCALSTACK_ENDPOINT)
E2E_ARGS            ?=

# LocalStack accepts any credentials; these keep a developer's real ones out of the suites.
AWS_ACCESS_KEY_ID     := test
AWS_SECRET_ACCESS_KEY := test

export LOCALSTACK_ENDPOINT FACTBUS_PG_URL FACTBUS_MYSQL_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

.PHONY: test up integration e2e down e2e-product down-product ci

test:
	$(PY) -m pytest

up:
	docker compose up -d --wait

integration:
	$(PY) -m pytest tests/integration

# One shell, so the destroy runs and the script's exit status survives it.
e2e:
	tofu -chdir=$(HELLO) init -input=false
	status=0; \
	tofu -chdir=$(HELLO) apply -input=false -auto-approve $(HELLO_VARS) || status=$$?; \
	if [ $$status -eq 0 ]; then \
		$(PY) scripts/e2e_hello.py --tf-dir $(HELLO) --endpoint $(LOCALSTACK_ENDPOINT) $(E2E_ARGS) || status=$$?; \
	fi; \
	if [ -z "$(KEEP)" ]; then \
		tofu -chdir=$(HELLO) destroy -input=false -auto-approve $(HELLO_VARS) || { [ $$status -ne 0 ] || status=1; }; \
	fi; \
	exit $$status

down:
	tofu -chdir=$(HELLO) destroy -input=false -auto-approve $(HELLO_VARS)

# The product example: one bus, one producer, one consumer; a fact staged in a SQL
# transaction is published after commit and applied once. Same shape as e2e.
e2e-product:
	tofu -chdir=$(PRODUCT) init -input=false
	status=0; \
	tofu -chdir=$(PRODUCT) apply -input=false -auto-approve $(PRODUCT_VARS) || status=$$?; \
	if [ $$status -eq 0 ]; then \
		$(PY) scripts/e2e_product.py --tf-dir $(PRODUCT) --endpoint $(LOCALSTACK_ENDPOINT) || status=$$?; \
	fi; \
	if [ -z "$(KEEP)" ]; then \
		tofu -chdir=$(PRODUCT) destroy -input=false -auto-approve $(PRODUCT_VARS) || { [ $$status -ne 0 ] || status=1; }; \
	fi; \
	exit $$status

down-product:
	tofu -chdir=$(PRODUCT) destroy -input=false -auto-approve $(PRODUCT_VARS)

# The local CI: no hosted runner needed. Stops at the first failing gate.
ci: test integration e2e e2e-product
	python3 scripts/check_eventing_boundaries.py
	PYTHON=$$(uv python find 3.12) bash scripts/test_sqlalchemy_matrix.sh
	cd iac/modules/consumer && tofu init -backend=false -input=false >/dev/null && tofu test
	cd $(PRODUCT) && tofu init -backend=false -input=false >/dev/null && tofu test
