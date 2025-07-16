"""
AirlineAWS Source Code Package

This package contains all the source code for the AirlineAWS data engineering
pipeline, including data ingestion, processing, machine learning, monitoring,
and orchestration modules.

Author: A Abhinav Jaisiv
Date: 2025
"""

# Import main modules
from . import data_ingestion
from . import data_processing
from . import machine_learning
from . import monitoring
from . import orchestration

__version__ = "1.0.0"
__author__ = "A Abhinav Jaisiv"

__all__ = [
    'data_ingestion',
    'data_processing', 
    'machine_learning',
    'monitoring',
    'orchestration'
] 