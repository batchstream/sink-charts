HELM ?= helm
PYTHON ?= python3
CHART := charts/sink

.PHONY: lint test package check

lint:
	$(HELM) lint $(CHART) --strict
	@for role in engine gateway worker; do \
		$(HELM) lint $(CHART) --strict -f examples/$$role-values.yaml || exit 1; \
	done

test:
	$(PYTHON) -m unittest discover -s tests -v

package:
	$(HELM) package $(CHART) --destination dist

check: lint test package
