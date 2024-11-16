# Fuzzy-Matching-using-fast-method
As fuzzy matching for large datasets are slow. I aim to fasten the speed of this process, so that researchers can run it quickly. 

I use rapidfuzz and multiprocessing to fasten. It fastens the time from a day to 15 minutes.

Overview
This repository provides a Python-based tool for matching organization names from two datasets (e.g., patents and pitchbooks) using fuzzy string matching and multiprocessing. The script is optimized for performance using shared memory and batch processing.

The primary objective is to:

Standardize organization names for better matching.
Match names from two datasets based on similarity.
Merge and output the results in a structured format.
