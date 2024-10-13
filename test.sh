#!/bin/bash
# bash script

# Run the python scripts in order
python3 jira_extract.py
python3 jira_transform.py
python3 jira_load.py