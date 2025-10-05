install:
	pip install -r requirements.txt

run:
	python src/index.py

test:
	pytest

lint:
	flake8 app
