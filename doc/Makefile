# Makefile for Sphinx documentation
#

# PYVER needs to be major.minor, just "3" doesn't work - it will result in
# issues with the amendments to PYTHONPATH and install paths (see DIST_VARS).

# Use explicit "version_info" indexing since make cannot handle colon characters, and
# evaluate it now to allow easier debugging when printing the variable

PYVER:=$(shell python3 -c 'from sys import version_info as v; print("{0}.{1}".format(v[0], v[1]))')
PYTHON = python$(PYVER)

# You can set these variables from the command line.
SPHINXOPTS    ?=
SPHINXBUILD   ?= LANG=C sphinx-build
PAPER         ?=
# # For merging a documentation archive into a git checkout of numpy/doc
# # Turn a tag like v1.18.0 into 1.18
# # Use sed -n -e 's/patttern/match/p' to return a blank value if no match
# TAG ?= $(shell git describe --tag | sed -n -e's,v\([1-9]\.[0-9]*\)\.[0-9].*,\1,p')

FILES=

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -T --keep-going -d build/doctrees $(PAPEROPT_$(PAPER)) \
                  $(SPHINXOPTS) source

.PHONY: help clean html version-check html-build

#------------------------------------------------------------------------------

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean     to remove generated doc files and start fresh"
	@echo "  html      to make standalone HTML files"

clean:
	-rm -rf build/*
	find . -name generated -type d -prune -exec rm -rf "{}" ";"


#------------------------------------------------------------------------------
# Automated generation of all documents
#------------------------------------------------------------------------------

# Build the current QToolKit version, and extract docs from it.
# We have to be careful of some issues:
#
# - Everything must be done using the same Python version
#

#SPHINXBUILD="LANG=C sphinx-build"


#------------------------------------------------------------------------------
# Basic Sphinx generation rules for different formats
#------------------------------------------------------------------------------
generate: build/generate-stamp
build/generate-stamp: $(wildcard source/reference/*.rst)
	mkdir -p build
	touch build/generate-stamp

generate-rst-from-md:
	# generate contributing docs
	mkdir -p source/dev
	pandoc --from=markdown --to=rst --output=source/dev/_contributing.rst ../CONTRIBUTING.md
	echo ".. _contributing:" > source/dev/contributing.rst
	cat source/dev/_contributing.rst >> source/dev/contributing.rst
	rm source/dev/_contributing.rst
	# generate changelog
	pandoc --from=markdown --to=rst --output=source/_changelog.rst ../CHANGELOG.md
	echo ".. _changelog:" > source/changelog.rst
	cat source/_changelog.rst >> source/changelog.rst
	rm source/_changelog.rst

html: api-doc html-build
html-build: generate generate-rst-from-md
	mkdir -p build/html build/doctrees
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) build/html $(FILES)
	@echo
	@echo "Build finished. The HTML pages are in build/html."
api-doc:
	sphinx-apidoc -e -f -o source/api ../src/jobflow_remote
