PY ?= python3
VENV := .venv

.PHONY: dev test wheel clean

$(VENV)/bin/activate:
	$(PY) -m venv $(VENV)
	. $(VENV)/bin/activate; pip install -U pip maturin pytest numpy

dev: $(VENV)/bin/activate
	. $(VENV)/bin/activate; maturin develop -r

test: $(VENV)/bin/activate
	. $(VENV)/bin/activate; pytest -q

wheel: $(VENV)/bin/activate
	. $(VENV)/bin/activate; maturin build -r

clean:
	rm -rf $(VENV) target/wheels
