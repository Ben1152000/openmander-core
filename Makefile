# Top-level Makefile for openmander bindings (Python and WASM)
# Usage examples:
#   make python-dev                # build & install Python bindings in venv (default MODE=release)
#   make python-dev MODE=dev       # debug build via maturin develop
#   make python-wheel TARGET=universal2-apple-darwin
#   make python-test
#   make wasm                      # build WASM bindings (default MODE=release)
#   make wasm MODE=dev             # debug build for WASM
#   make wasm-copy                 # build WASM and copy to openmander-app
#   make wasm-copy MODE=dev        # debug build and copy
#   make clean / make clean-all
#
# Notes:
# - BINDINGS_DIR points at the pyo3 crate
# - WASM_BINDINGS_DIR points at the wasm-bindgen crate
# - APP_DIR points to openmander-app for copying WASM output
# - MODE controls --release flag (release or dev)
# - TARGET passes --target to maturin (e.g., universal2-apple-darwin, x86_64-apple-darwin)
# - WASM_TARGET is the wasm-pack target (default: web)
#
# This Makefile was generated using AI (use at your own risk)
SHELL := /bin/bash

# -------- configurable --------
PY              ?= python3
VENV            ?= bindings/python/.venv
BINDINGS_DIR    ?= bindings/python
WASM_BINDINGS_DIR ?= bindings/wasm
APP_DIR         ?= ../openmander-app
MODE            ?= release          # 'release' or 'dev'
TARGET          ?=                  # e.g. universal2-apple-darwin, x86_64-apple-darwin, aarch64-apple-darwin
WASM_TARGET     ?= web               # wasm-pack target: web, bundler, nodejs, no-modules
WASM_OUT_DIR    ?= $(APP_DIR)/wasm/pkg

# -------- platform tweaks (macOS/Linux vs Windows) --------
VENV_BIN := $(VENV)/bin
ifeq ($(OS),Windows_NT)
  VENV_BIN := $(VENV)/Scripts
  PY       := py -3
endif

# -------- derived --------
PYTHON          := $(VENV_BIN)/python
PIP             := $(PYTHON) -m pip
MATURIN         := $(VENV_BIN)/maturin
MANIFEST        := -m $(BINDINGS_DIR)/Cargo.toml
PROFILE_FLAG    := $(if $(filter $(MODE),release),--release,)
TARGET_FLAG     := $(if $(TARGET),--target $(TARGET),)
WHEEL_DIR       := target/wheels
WASM_PACK       := wasm-pack
WASM_PROFILE    := $(if $(filter $(MODE),release),--release,)
WASM_TARGET_FLAG := --target $(WASM_TARGET)
WASM_OUT        := $(WASM_BINDINGS_DIR)/pkg

# Make `help` the default when you just run `make`
.DEFAULT_GOAL := help

.PHONY: all venv python-deps python-dev python-wheel python-test python-otool \
        wasm wasm-copy wasm-clean \
        clean clean-all print-vars help prepare-target check-wasm-pack

# ============================================================================
# Build All
# ============================================================================

# Build all bindings (Python and WASM)
all: python-dev wasm-copy
	@echo "✅ All bindings built successfully!"

# ============================================================================
# Python Bindings
# ============================================================================

# Create/refresh the virtual environment (but don't auto-install deps here)
venv: $(PYTHON)
$(PYTHON):
	$(PY) -m venv $(VENV)
	source $(VENV)/bin/activate && \
	$(PIP) install -U pip

# Install Python-side build/test deps into the venv
python-deps: venv
	$(PIP) install -U maturin ipykernel

# (Optional) ensure rust target is available if TARGET is set
prepare-target:
ifneq ($(TARGET),)
	rustup target add $(TARGET) || true
endif

# Build & install the package into the venv (like editable)
python-dev: python-deps prepare-target
	source $(VENV)/bin/activate && \
	$(MATURIN) develop $(PROFILE_FLAG) $(TARGET_FLAG) $(MANIFEST)

# Build wheel(s) into target/wheels
python-wheel: python-deps prepare-target
	source $(VENV)/bin/activate && \
	$(MATURIN) build $(PROFILE_FLAG) $(TARGET_FLAG) $(MANIFEST)

# Run tests (from the bindings dir so relative tests work)
python-test: python-deps
	cd $(BINDINGS_DIR) && $(PYTHON) -m pytest -q

# Inspect the loaded .so on macOS (prints path then runs otool -L)
python-otool:
	@SO=$$($(PYTHON) - <<'PY'\nimport openmander; print(openmander.__file__)\nPY\n); \
	echo $$SO; otool -L "$$SO"

# Legacy aliases for backward compatibility
deps: python-deps
dev: python-dev
wheel: python-wheel
test: python-test
otool: python-otool

# ============================================================================
# WASM Bindings
# ============================================================================

# Check if wasm-pack is installed
check-wasm-pack:
	@which $(WASM_PACK) > /dev/null || (echo "Error: wasm-pack not found. Install with: cargo install wasm-pack" && exit 1)

# Build WASM bindings (outputs to bindings/wasm/pkg)
wasm: check-wasm-pack
	cd $(WASM_BINDINGS_DIR) && \
	$(WASM_PACK) build $(WASM_PROFILE) $(WASM_TARGET_FLAG) --out-dir pkg

# Build WASM bindings and copy to openmander-app/wasm/pkg
wasm-copy: wasm
	@echo "Copying WASM bindings to $(WASM_OUT_DIR)..."
	@mkdir -p $(WASM_OUT_DIR)
	@cp -r $(WASM_OUT)/* $(WASM_OUT_DIR)/
	@echo "WASM bindings copied successfully!"

# Clean WASM build artifacts
wasm-clean:
	rm -rf $(WASM_OUT)
	rm -rf $(WASM_BINDINGS_DIR)/target

# Shallow clean: venv & wheels
clean:
	rm -rf $(VENV) $(WHEEL_DIR)

# Deep clean: also clear Cargo artifacts and stray egg/dist-info
clean-all: clean wasm-clean
	cargo clean
	find . -name '*.egg-info' -o -name '*.dist-info' -type d -prune -exec rm -rf {} +

print-vars:
	@echo 'Python bindings:'
	@echo '  PY=$(PY)'
	@echo '  VENV=$(VENV)'
	@echo '  VENV_BIN=$(VENV_BIN)'
	@echo '  BINDINGS_DIR=$(BINDINGS_DIR)'
	@echo '  MODE=$(MODE)'
	@echo '  TARGET=$(TARGET)'
	@echo '  PROFILE_FLAG=$(PROFILE_FLAG)'
	@echo '  TARGET_FLAG=$(TARGET_FLAG)'
	@echo '  MANIFEST=$(MANIFEST)'
	@echo ''
	@echo 'WASM bindings:'
	@echo '  WASM_BINDINGS_DIR=$(WASM_BINDINGS_DIR)'
	@echo '  APP_DIR=$(APP_DIR)'
	@echo '  WASM_OUT_DIR=$(WASM_OUT_DIR)'
	@echo '  WASM_TARGET=$(WASM_TARGET)'
	@echo '  WASM_PROFILE=$(WASM_PROFILE)'
	@echo '  WASM_OUT=$(WASM_OUT)'

help:
	@echo 'Python Bindings:'
	@echo '  python-deps        Install build/test deps into venv'
	@echo '  python-dev         Build & install in venv (MODE=release|dev, TARGET=...)'
	@echo '  python-wheel       Build wheel(s) (MODE=release|dev, TARGET=...)'
	@echo '  python-test        Run pytest from $(BINDINGS_DIR)'
	@echo '  python-otool        Show linked libs for installed .so (macOS)'
	@echo ''
	@echo '  Legacy aliases: deps, dev, wheel, test, otool (same as python-*)'
	@echo ''
	@echo 'WASM Bindings:'
	@echo '  wasm               Build WASM bindings (MODE=release|dev, WASM_TARGET=web|bundler|nodejs|no-modules)'
	@echo '  wasm-copy          Build WASM and copy to $(WASM_OUT_DIR)'
	@echo '  wasm-clean         Remove WASM build artifacts'
	@echo ''
	@echo 'Common:'
	@echo '  all                Build all bindings (Python + WASM with copy)'
	@echo '  venv               Create virtualenv'
	@echo '  clean              Remove venv and built wheels'
	@echo '  clean-all          Also remove Cargo target, WASM artifacts, and egg/dist-info'
	@echo '  print-vars         Show all configuration variables'
	@echo ''
	@echo 'Variables:'
	@echo '  MODE                Build mode: release (default) or dev'
	@echo '  TARGET             Rust target for Python bindings (e.g., universal2-apple-darwin)'
	@echo '  WASM_TARGET        wasm-pack target: web (default), bundler, nodejs, no-modules'
	@echo '  APP_DIR            Directory for openmander-app (default: ../openmander-app)'
	@echo ''
	@echo 'Examples:'
	@echo '  make all                            # Build all bindings (Python + WASM)'
	@echo '  make all MODE=dev                   # Debug build all bindings'
	@echo '  make python-dev                     # Release build Python bindings'
	@echo '  make python-dev MODE=dev            # Debug build Python bindings'
	@echo '  make wasm                           # Release build WASM bindings'
	@echo '  make wasm-copy MODE=dev             # Debug build and copy WASM'
	@echo '  make wasm WASM_TARGET=bundler       # Build for bundler target'
