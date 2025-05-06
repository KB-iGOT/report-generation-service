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

# Set environment variables for testing
os.environ.setdefault('PYTHONPATH', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Disable pandas warnings during tests
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'