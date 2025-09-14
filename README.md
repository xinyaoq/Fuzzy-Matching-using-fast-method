# Fuzzy-Matching-using-fast-method

## Overview
This repository provides a Python-based tool for matching organization names from two datasets (e.g., patents and pitchbooks) using fuzzy string matching and multiprocessing. The script is optimized for performance using shared memory and batch processing.

The primary objective is to:
1. Standardize organization names for better matching.
2. Match names from two datasets based on similarity.
3. Merge and output the results in a structured format.

## Features
``Fuzzy Matching``: Uses rapidfuzz for efficient string similarity calculations.  
``Multiprocessing``: Leverages multiple CPU cores for parallel processing.  
``Shared Memory``: Utilizes multiprocessing.shared_memory to minimize data duplication across processes.  
``Batch Processing``: Handles large datasets efficiently by processing them in chunks.  

**1. Install libraries**  
```bash
pip install pandas numpy rapidfuzz tqdm
```
**2. Standardize company's name**
```bash
def standardize_text(text):
    if pd.isnull(text):
        return ''
    # Convert to lowercase
    text = text.lower()
    # Remove special characters
    text = re.sub(r'[^\w\s]', '', text)
    # Remove extra spaces
    text = ' '.join(text.split())
    return text
```
**3. Optimized match-and-merge function using cdist**
```bash
def match_and_merge(rows, patent_stdlist, patent_df):
    results = []
    row_names = [row['companyname_std'] for row in rows]
    matches = process.cdist(row_names, patent_stdlist, scorer=fuzz.ratio, workers=-1)  # Vectorized matching
    
    for i, row in enumerate(rows):
        best_match_idx = np.argmax(matches[i])  # Best match index
        best_match_score = matches[i][best_match_idx]
        
        if best_match_score > 90:
            # Retrieve the matched row from patent_df using the index
            matched_org = patent_df.iloc[best_match_idx]
            row_series = pd.Series(row)
            merged_row = pd.concat([row_series, matched_org], axis=0)
            results.append(merged_row.tolist() + [1])
        else:
            # No match above threshold; fill with NaN
            row_series = pd.Series(row)
            empty_series = pd.Series([None] * len(patent_df.columns), index=patent_df.columns)
            merged_row = pd.concat([row_series, empty_series], axis=0)
            results.append(merged_row.tolist() + [0])
    return results
```
**4. Multiprocessing**
```bash
def process_single_batch(batch, patent_stdlist_shm_name, patent_stdlist_shape, patent_df, pitchbook_columns, dtype_str):
    existing_shm = shared_memory.SharedMemory(name=patent_stdlist_shm_name)
    patent_stdlist = np.ndarray(patent_stdlist_shape, dtype=dtype_str, buffer=existing_shm.buf)
    results = match_and_merge(batch, patent_stdlist.tolist(), patent_df)
    return pd.DataFrame(results, columns=pitchbook_columns + patent_df.columns.tolist() + ['merge_flag'])
```
**5. Main**  
The script processes large datasets by dividing them into smaller batches. This approach minimizes memory usage and allows for efficient parallel processing using Python's multiprocessing module.

 

