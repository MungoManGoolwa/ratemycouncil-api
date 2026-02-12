#!/usr/bin/env python3
import sys
import os

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import the WSGI application
from application import application