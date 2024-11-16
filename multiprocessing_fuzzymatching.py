# using this one
import pandas as pd
import numpy as np
from rapidfuzz import process
from rapidfuzz import fuzz
from tqdm import tqdm
import re
import multiprocessing as mp
from multiprocessing import shared_memory
import io

# Function to standardize organization names
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

# Optimized match-and-merge function using cdist
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

# Wrapper for multiprocessing
def process_single_batch(batch, patent_stdlist_shm_name, patent_stdlist_shape, patent_df, pitchbook_columns, dtype_str):
    existing_shm = shared_memory.SharedMemory(name=patent_stdlist_shm_name)
    patent_stdlist = np.ndarray(patent_stdlist_shape, dtype=dtype_str, buffer=existing_shm.buf)
    results = match_and_merge(batch, patent_stdlist.tolist(), patent_df)
    return pd.DataFrame(results, columns=pitchbook_columns + patent_df.columns.tolist() + ['merge_flag'])

def main():
    output_file = "final_patent_patent_pitchbook.csv"
    batch_size = 100  # Increased batch size
#     num_processes = mp.cpu_count() - 1  # Use all but one core
    num_processes = 10

    # Load and standardize datasets
    patent = pd.read_csv('unique_patent_with_uniqueid.csv')
    pitchbook = pd.read_csv('pitchbook_withstdname.csv')
    
    # Standardize organization names
    patent['disambig_assignee_organization_std'] = patent['disambig_assignee_organization'].apply(standardize_text)
    max_length = patent['disambig_assignee_organization_std'].str.len().max()
    dtype_str = f'U{max_length}'
    patent_stdlist = np.array(patent['disambig_assignee_organization_std'].tolist(), dtype=dtype_str)  

    # Shared memory setup for patent_stdlist
    patent_stdlist_shm = shared_memory.SharedMemory(create=True, size=patent_stdlist.nbytes)
    shm_array = np.ndarray(patent_stdlist.shape, dtype=patent_stdlist.dtype, buffer=patent_stdlist_shm.buf)
    shm_array[:] = patent_stdlist[:]

    # Create batches
    batches = [pitchbook[i:i + batch_size].to_dict(orient='records') for i in range(0, len(pitchbook), batch_size)]
    pitchbook_columns = pitchbook.columns.tolist()

    header_written = False
    buffer = io.StringIO()  # In-memory buffer to batch write results

    # Process batches in parallel
    with mp.Pool(num_processes) as pool:
        for batch_result in tqdm(pool.starmap(process_single_batch, [
            (batch, patent_stdlist_shm.name, patent_stdlist.shape, patent, pitchbook_columns, dtype_str) for batch in batches
        ])):
            batch_result.to_csv(buffer, mode='a', index=False, header=not header_written)
            header_written = True  # Only write header once

            # Periodically flush buffer to file to reduce I/O frequency
            if buffer.tell() > 10_000_000:  # Flush every ~10MB of data
                with open(output_file, 'a') as f:
                    f.write(buffer.getvalue())
                buffer.seek(0)
                buffer.truncate(0)

    # Final buffer flush
    if buffer.tell() > 0:
        with open(output_file, 'a') as f:
            f.write(buffer.getvalue())

    # Cleanup shared memory
    patent_stdlist_shm.close()
    patent_stdlist_shm.unlink()  # Unlink to release memory

if __name__ == '__main__':
    import time
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")