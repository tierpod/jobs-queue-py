PY ?= 3

# outside venv
venv:
	python$(PY) -m venv $@

# inside venv
.PHONY: init-dev
init-dev:
	pip install -U -r requirements-dev.txt
	pip install -U --editable .

init:
	pip install -U -r requirements.txt

.PHONY: archive
archive:
	python setup.py sdist
