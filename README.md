Project: Job Profile Analysis
=========

## Description
This repository contains the artefacts requested as part of the application process for a certain data role

The report below is the primary artefact for this project, other artefects are listed in the table of contents
- [job-profile-analysis](/job-profile-analysis.ipynb)

## Table of contents
<!--ts-->
   * [Local Setup](#local-setup)
   * [Run Tests Locally](#run-tests-locally)
   * [Continuous Integration Testing with Github Actions](#continuous-integration-testing-with-github-actions)
   * [Logging](#logging)

Local Setup
============

Assumed prequisites
- Docker is installed (preferbly newer than version 23.0.5)

Download the data into the test_data directory and unzip it
- The unzipped data should appear as below

![alt text](/images/test-data-file-structure.png)


Run the command below
```
docker run -it --rm -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/pyspark-notebook
```

Click the link that comes up as below or copy and paste it into a browser
![alt text](/images/notebook-url.png)

Click the work directory in the left panel
![alt text](/images/click-work.png)

Click the job-profile-analysis.ipynb
![alt text](/images/click-analysis.png)


Run Tests Locally
============

Assumed prequisites
- One of python 3.7/3.8/3.9/3.10 are installed

You can run the tests using pytests and tox, we will use tox
- Tox allows us to run our tests for all configured versions of python
- This configuration is in tox.ini
  - ![alt text](/images/tox-ini.png)
- Ideally you will have all these python versions installed, but if you don't it will just raise an invocation error for that version
  - If you have none of the versions then no tests will run
  - Note - 3.7 will give warnings as the current version of spark does not support python 3.7

Add the current directory to the PYTHONPATH by running the command below
```
export PYTHONPATH=$PYTHONPATH:${PWD}
```

Setup a python virtual environment & install python packages
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To run the tests simply run the command below
- Keep in mind that the first time you run this it can take some because Tox will setup an individual virtual environment for each version of python and install packages to each of them
```
tox
```
![alt text](/images/tox-testing.png)


Continuous Integration Testing with Github Actions
============

You can find the configuration for the github action below
- [/.github/workflows/run_tests.yml)](/.github/workflows/run_tests.yml)

You can view the results here
- [Actions](https://github.com/hugh-nguyen/job-profiles-analysis/actions)

The action sets up a linux server with Spark and python 3.7, 3.8, 3.9, 3.10 and runs all of the tests with tox

![alt text](/images/tox-github.png)


Logging
============

Logging is an extremely part of any data pipeline or analytics
- A simple logging decorator has been setup to write to the logs directory
- It will log everytime a function is started, completed and whenever it fails with a message

![alt text](/images/logging.png)
