HELM ?= helm
PYTHON ?= python3
SCENARIO ?= all
CHART := charts/sink

.PHONY: lint test package check integration
lint:
	$(HELM) lint $(CHART) --strict
	$(HELM) lint $(CHART) --strict -f examples/cluster-values.yaml

test:
	$(PYTHON) -m unittest discover -s tests -v

package:
	$(HELM) package $(CHART) --destination dist

check: lint test package

# Explicit opt-in: creates and destroys its own local Kind cluster.
integration:
	$(PYTHON) tests/integration/run.py --scenario $(SCENARIO)
