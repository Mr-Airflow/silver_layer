# utils/silver_io.py
#
# This module has been superseded by BulkTransformEngine in transform_engine.py.
#
# WatermarkManager, IncrementalLoadEngine, MultiSourceReader, and SilverWriter
# were the single-table YAML-driven pipeline helpers.  The entire pipeline now
# uses the CSV-driven BulkTransformEngine which manages watermarks, writes, and
# multi-source SQL directly.
#
# File retained as an empty stub to avoid import errors during any transition.
