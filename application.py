#!/usr/bin/env python3
import sys
import os

# Add the backend directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import the FastAPI app
from main import app

# For WSGI deployment
application = app