"""
Data Processing Module for AirlineAWS Project

This module provides comprehensive data processing capabilities including
ETL pipelines, data quality validation, and transformation logic.

Author: A Abhinav Jaisiv
Date: 2025
"""

from .etl_pipeline import ETLPipeline, DataQualityConfig

__all__ = ['ETLPipeline', 'DataQualityConfig'] 