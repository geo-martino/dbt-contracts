# Development commands, plus the Sphinx documentation targets
#

# You can set these variables from the command line, and also
# from the environment for the first two.
VERSION       = $(shell uv run hatch version)
SPHINXOPTS    ?= -D release=${VERSION}
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = docs
BUILDDIR      = docs/_build
LINKCHECKDIR  = docs/_linkcheck

# Put it first so that "make" without argument is like "make help".
help:
	@echo "Development commands:"
	@echo "  install       Install the package with all dependency groups"
	@echo "  test          Run the tests"
	@echo "  lint          Check the code with ruff"
	@echo "  lint-fix      Check the code with ruff and fix what it can"
	@echo "  docs          Build the HTML documentation, failing on any warnings"
	@echo "  check         Run lint, test and docs i.e. everything CI validates"
	@echo "  rebuild-html  Clean and rebuild the HTML documentation, then check its links"
	@echo
	-@uv run $(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

install:
	uv sync

test:
	uv run pytest -n auto -m "not manual"

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

docs:
	uv run python "$(SOURCEDIR)"/contracts.py
	uv run $(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) -W --keep-going $(O)

check: lint test docs

linkcheck: Makefile
	@$(SPHINXBUILD) -b linkcheck "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

rebuild-html: Makefile
	@$(SPHINXBUILD) -M clean "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@rm -rf "$(SOURCEDIR)"/reference/api
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@$(SPHINXBUILD) -b linkcheck "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help install test lint lint-fix docs check linkcheck rebuild-html Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
