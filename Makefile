# Top-level Makefile for openmander python bindings
# Usage examples:
#   make dev                       # build & install in venv (default MODE=release)
#   make dev MODE=dev              # debug build via maturin develop
#   make wheel TARGET=universal2-apple-darwin
#   make test
#   make clean / make clean-all
#
# Notes:
# - BINDINGS_DIR points at the pyo3 crate
# - MODE controls --release flag
# - TARGET passes --target to maturin (e.g., universal2-apple-darwin, x86_64-apple-darwin)
#
# This Makefile was generated using AI (use at your own risk)
SHELL := /bin/bash

# -------- configurable --------
PY           ?= python3
VENV         ?= bindings/python/.venv
BINDINGS_DIR ?= bindings/python
MODE         ?= release          # 'release' or 'dev'
TARGET       ?=                  # e.g. universal2-apple-darwin, x86_64-apple-darwin, aarch64-apple-darwin

# -------- platform tweaks (macOS/Linux vs Windows) --------
VENV_BIN := $(VENV)/bin
ifeq ($(OS),Windows_NT)
  VENV_BIN := $(VENV)/Scripts
  PY       := py -3
endif

# -------- derived --------
PYTHON       := $(VENV_BIN)/python
PIP          := $(PYTHON) -m pip
MATURIN      := $(VENV_BIN)/maturin
MANIFEST     := -m $(BINDINGS_DIR)/Cargo.toml
PROFILE_FLAG := $(if $(filter $(MODE),release),--release,)
TARGET_FLAG  := $(if $(TARGET),--target $(TARGET),)
WHEEL_DIR    := target/wheels

# Make `help` the default when you just run `make`
.DEFAULT_GOAL := help

.PHONY: venv deps dev wheel test otool clean clean-all print-vars help prepare-target

# Create/refresh the virtual environment (but don't auto-install deps here)
venv: $(PYTHON)
$(PYTHON):
	$(PY) -m venv $(VENV)
	source $(VENV)/bin/activate && \
	$(PIP) install -U pip

# Install Python-side build/test deps into the venv
deps: venv
	$(PIP) install -U maturin ipykernel

# (Optional) ensure rust target is available if TARGET is set
prepare-target:
ifneq ($(TARGET),)
	rustup target add $(TARGET) || true
endif

# Build & install the package into the venv (like editable)
dev: deps prepare-target
	source $(VENV)/bin/activate && \
	$(MATURIN) develop $(PROFILE_FLAG) $(TARGET_FLAG) $(MANIFEST)

# Build wheel(s) into target/wheels
wheel: deps prepare-target
	source $(VENV)/bin/activate && \
	$(MATURIN) build $(PROFILE_FLAG) $(TARGET_FLAG) $(MANIFEST)

# Run tests (from the bindings dir so relative tests work)
test: deps
	cd $(BINDINGS_DIR) && $(PYTHON) -m pytest -q

# Inspect the loaded .so on macOS (prints path then runs otool -L)
otool:
	@SO=$$($(PYTHON) - <<'PY'\nimport openmander; print(openmander.__file__)\nPY\n); \
	echo $$SO; otool -L "$$SO"

# Shallow clean: venv & wheels
clean:
	rm -rf $(VENV) $(WHEEL_DIR)

# Deep clean: also clear Cargo artifacts and stray egg/dist-info
clean-all: clean
	cargo clean
	find . -name '*.egg-info' -o -name '*.dist-info' -type d -prune -exec rm -rf {} +

print-vars:
	@echo PY=$(PY)
	@echo VENV=$(VENV)
	@echo VENV_BIN=$(VENV_BIN)
	@echo BINDINGS_DIR=$(BINDINGS_DIR)
	@echo MODE=$(MODE)
	@echo TARGET=$(TARGET)
	@echo PROFILE_FLAG=$(PROFILE_FLAG)
	@echo TARGET_FLAG=$(TARGET_FLAG)
	@echo MANIFEST=$(MANIFEST)

help:
	@echo 'Targets:'
	@echo '  venv            Create virtualenv'
	@echo '  deps            Install build/test deps into venv'
	@echo '  dev             maturin develop (MODE=release|dev, TARGET=...)'
	@echo '  wheel           maturin build   (MODE=release|dev, TARGET=...)'
	@echo '  test            Run pytest from $(BINDINGS_DIR)'
	@echo '  otool           Show linked libs for installed .so (macOS)'
	@echo '  clean           Remove venv and built wheels'
	@echo '  clean-all       Also remove Cargo target and egg/dist-info'
	@echo 'Vars: PY, VENV, BINDINGS_DIR, MODE, TARGET'
