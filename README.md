# Abigail Byram: Singlestone Data Python Exercise

## Purpose
This project is to showcase some simple data manipulation in a Python package using Pyspark and unit testing with Pytest.

## Run Instructions
All instructions assume you are running from the project root.

To install required dependncies, run the following: 

```
pip install -r requirements.txt
```

There are two ways to run the app:

1. Direct Python package run:
```
python data_processor <path/to/student/file> <path/to/teacher/file> --out <optional/output/path>
```

2. Pipenv script
```
pipenv run generate-report
```

Note that when an output path is not specified, the output file name defaults to `report.json`

Finally, unit testing can be run with the command
```
pipenv run unit-test
```