# dalite-data
Utility script for processing data from the dalite LTI tool (https://github.com/open-craft/dalite-ng)

## About
dalite.py contains utilities for extracting database tables from the raw database json export, parsing split log files, and combining them into an output table where each row contains data on a student response.

## Requirements
* pandas (http://pandas.pydata.org/)

## Basic usage

1. Download and unzip the latest json.gz data file (typically named with the pattern `database-YYYYMMDDTHH:MM:SS.json[.gz]`)

2. Download all log files (typically named with the pattern `student.log-YYYYMMDD`)

3. Run dalite.py, specifying the json data file, the logs directory containing the student log files, and the output csv file to write to.

```
Usage: dalite.py [-h] [--database-json DATABASE_JSON]
             [--logs-directory LOGS_DIRECTORY] [--output-file OUTPUT_FILE]

Arguments:
  -h, --help            show this help message and exit
  --database-json DATABASE_JSON
                        .json or .json.gz file containing database table data
                        (typically labelled "database-
                        YYYYMMDDTHH:MM:SS.json[.gz]")
  --logs-directory LOGS_DIRECTORY
                        directory containing log files (containing files
                        typically labelled "student.log-YYYYMMDD")
  --output-file OUTPUT_FILE
                        filename for joined output csv file (e.g.
                        "output.csv")
```

Example usage:
```
python dalite.py --database-json data/db/database-20160826T06\:43\:18.json --logs-directory data/logs --output-file output/output.csv
```