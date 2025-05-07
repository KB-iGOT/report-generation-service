import pytest
import sys
import os

# First import werkzeug, then set its version
try:
    import werkzeug
    werkzeug.__version__ = '2.0.0'
except ImportError:
    # If werkzeug is not installed, we don't need to mock its version
    pass

# Add the project root directory to Python's path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Set environment variables for testing
os.environ.setdefault('PYTHONPATH', project_root)

# Disable pandas warnings during tests
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

# Print debug information
print(f"Python path: {sys.path}")
print(f"Project root: {project_root}")
print(f"Current directory: {os.getcwd()}")
