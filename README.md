# Fuzzy-Matching-using-fast-method

## Overview
This repository provides a Python-based tool for matching organization names from two datasets (e.g., patents and pitchbooks) using fuzzy string matching and multiprocessing. The script is optimized for performance using shared memory and batch processing.

The primary objective is to:
1. Standardize organization names for better matching.
2. Match names from two datasets based on similarity.
3. Merge and output the results in a structured format.

## Features
**Fuzzy Matching**: Uses rapidfuzz for efficient string similarity calculations.
**Multiprocessing**: Leverages multiple CPU cores for parallel processing.
**Shared Memory**: Utilizes multiprocessing.shared_memory to minimize data duplication across processes.
**Batch Processing**: Handles large datasets efficiently by processing them in chunks.

