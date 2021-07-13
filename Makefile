.DEFAULT_GOAL: all

all:
	pip install -r requirements.txt
	python parse.py --dir perfdata/
	python upload.py --url http://.../api/put
	rm icinga.service.*
