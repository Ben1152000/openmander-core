PY ?= python3
VENV := .venv

.PHONY: dev test wheel clean

# Create the virtual environment and install dependencies
$(VENV)/bin/activate:
	$(PY) -m venv $(VENV)
	. $(VENV)/bin/activate; pip install -U pip maturin pytest numpy

# Build and install the python package in the current venv
dev: $(VENV)/bin/activate
	. $(VENV)/bin/activate; maturin develop -r

# Run tests
test: $(VENV)/bin/activate
	. $(VENV)/bin/activate; pytest -q

# Build a release wheel
wheel: $(VENV)/bin/activate
	. $(VENV)/bin/activate; maturin build -r

clean:
	rm -rf $(VENV) target/wheels
