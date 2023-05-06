# job-profiles-analysis

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export PYTHONPATH=${PWD}

docker run -it --rm -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/pyspark-notebook