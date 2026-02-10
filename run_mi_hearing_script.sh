#!/usr/bin/env bash
#!/bin/bash

# Navigate to project directory
cd "$(dirname "$0")"

# Activate virtual environment
source venv/bin/activate

# Run the transcriber
python3 main.py "$@"

# Exit with the same code as the Python script
exit $?