#!/bin/bash
# bash script

# Run the python scripts in order
echo "Executing extract script..."
python3 jira_extract.py

echo "Executing transform script..."
python3 jira_transform.py

echo "Executing load script..."
python3 jira_load.py