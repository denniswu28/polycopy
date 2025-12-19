PYTHON ?= python

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	PYTHONPATH=src $(PYTHON) -m pytest

run:
	PYTHONPATH=src $(PYTHON) -m polycopy.main

healthcheck:
	PYTHONPATH=src $(PYTHON) -m polycopy.main --healthcheck
