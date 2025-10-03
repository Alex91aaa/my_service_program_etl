#%% notes



#%% modules
import os
os.chdir(r'C:\Projekt\service\2_program\service_program')

import shutil
import logging
import yaml
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import re
from scipy.interpolate import interp1d

#%% classes

   

#%% functions

def load_yaml(file):
    # os.chdir(r'C:\Projekt\service\2_program\service_program')
    with open(f"{file}.yaml", "r") as f:
        return yaml.safe_load(f) 

config = load_yaml('config')

def create_logging(stage):
    # Load configuration
    config = load_yaml('config')

    # Create the log directory path based on today's date
    log_path = os.path.join(
        config['program_file_directories']['base_target'],
        config['program_file_directories']['log_files'],
        datetime.strftime(datetime.today(), "%Y%m%d")
    )

    # Ensure the directory exists
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    # Set up the logger for the specific stage
    logger = logging.getLogger(stage)  # Use the stage name as the logger name
    logger.setLevel(logging.INFO)  # Set the logging level to INFO or as needed

    # Create a file handler for this stage
    file_handler = logging.FileHandler(os.path.join(log_path, f"{stage}.log"))
    file_handler.setLevel(logging.INFO)  # Log level for this handler

    # Define log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    if not logger.handlers:  # Avoid adding multiple handlers if the logger is reused
        logger.addHandler(file_handler)

    return logger

def create_subdirectories(directory_path, day):
    sub_directory = os.path.join(directory_path, day)
    os.makedirs(sub_directory, exist_ok=True)
    
def retry(func, log_stage = None, retries = 5, delay = 2):
    if not log_stage is None:
        logger = create_logging(config['log_stages'][log_stage])
    errors = []
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            if not log_stage is None:
                logger.error(f"An error occurred: {e}")
            errors.append(e)
            if i < retries - 1:
                time.sleep(delay)
            else:
                # Log the error or raise it on the final retry attempt
                raise e
    return errors

# def generate_history(db_existing_data, days_to_check = 1):
#     today = datetime.today() - timedelta(days = config['fetch_config']['craw_day_delay'])
#     history = []
#     day = today
    
#     while len(history) < days_to_check:
#         formatted_day = day.strftime("%Y%m%d")
#         # Add to history if it's not in db_existing_data
#         if formatted_day not in db_existing_data:
#             history.append(formatted_day)
#         if formatted_day == config['fetch_config']['end_date']:
#             break 
#         day -= timedelta(days=1)

#     return history

def generate_history(db_existing_data, days_to_check=1):
    today = datetime.today() - timedelta(days=config['fetch_config']['craw_day_delay'])
    history = []
    day = today
    no_valid_date = False  # Flag to track end_date condition

    while len(history) < days_to_check:
        formatted_day = day.strftime("%Y%m%d")

        if formatted_day not in db_existing_data:
            history.append(formatted_day)

        if formatted_day == config['fetch_config']['end_date']:
            no_valid_date = True
            break  # Stop checking history once end_date is met

        day -= timedelta(days=1)

    return history, no_valid_date
    
def get_days_between(start, end, strftime_format = '%d%m%Y'):
    start_date = datetime.strptime(start, '%d/%m/%Y')
    end_date = datetime.strptime(end, '%d/%m/%Y')
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime(strftime_format))
        current_date += timedelta(days=1)
    return dates

def generate_smooth_time_steps(start, end, threshold_step, delta, min_delta, num_points):
    """
    Generate a list of timesteps with smoothly increasing step sizes using a formula.

    Parameters:
    - start (int): The starting timestep.
    - end (int): The ending timestep.
    - initial_step (int): The base step size.
    - num_points (int): The approximate number of points to generate.

    Returns:
    - list: A list of timesteps with varying step sizes.
    """
    time_steps = []
    current_time = start

    while current_time < end and len(time_steps) < num_points:
        time_steps.append(current_time)
        delta_value = ((current_time / end) * delta) + min_delta
        step_size = threshold_step * current_time / end + delta_value # Avoid division by zero
        current_time += step_size

    return sorted(set(map(int, time_steps)))

########################################
# # Example usage
# start_time = 30
# end_time = 10500
# delta = 60
# min_delta = 10
# threshold_step = 80
# num_points = 300
# select_time_array = generate_smooth_time_steps(start_time, end_time, threshold_step, delta, min_delta, num_points)
########################################

def normalize_transaction_value_data(df, start_def, start_time, end_time, select_time_array):
    ids = df['num_service'].unique()
    start = int(start_def.timestamp())
    # start_def_name = 'realstart_'

    transaction_value_normalized_data = pd.DataFrame()

    for num_service in ids:
        result_temp = pd.DataFrame()

        data_num_service = df.loc[df['num_service'] == num_service]
        
        data_num_service_copy = data_num_service.copy()
        data_num_service_copy['update_time'] = data_num_service_copy['update_time'].astype('int64') // 10**9
        # data_num_service_copy['update_time'] = data_num_service_copy['update_time'] / 1000

        f = interp1d(data_num_service_copy['update_time'], data_num_service_copy['transaction_value_estimated'], fill_value='extrapolate')
        
        t_10 = np.arange(start - (end_time + 1), start - start_time, 1)

        y_10 = f(t_10)

        first_updateTime = start - data_num_service_copy['update_time'].min()
        last_updateTime = start - data_num_service_copy['update_time'].max()
 
        updateTime_standardize = t_10 - (start - (end_time + 31))
        reversed_updateTime_standardize = updateTime_standardize[::-1]

        transaction_value_ratio_standardize_all = pd.DataFrame({'time': reversed_updateTime_standardize, 'transaction_value_estimated': y_10})

        transaction_value_standardize_selected = {}
        
        for element in select_time_array:
            column_name = 'time_' + str(transaction_value_ratio_standardize_all.loc[transaction_value_ratio_standardize_all['time'] == element, 'time'].values[0])
            value = transaction_value_ratio_standardize_all.loc[transaction_value_ratio_standardize_all['time'] == element, 'transaction_value_estimated'].values[0]
            transaction_value_standardize_selected[column_name] = [value]
            
        transaction_value_standardize_selected = pd.DataFrame(transaction_value_standardize_selected)

        for column in transaction_value_standardize_selected.columns:
            second_part = int(column.split('_')[1])
            if second_part > first_updateTime:
                transaction_value_standardize_selected[column] = np.nan
            if second_part < last_updateTime:
                transaction_value_standardize_selected[column] = np.nan
        
        # transaction_value_standardize_selected = transaction_value_standardize_selected.rename(columns=lambda x: start_name + x)
        transaction_value_standardize_selected = transaction_value_standardize_selected.copy()
        transaction_value_standardize_selected['num_service'] = num_service
       
        result_temp = transaction_value_standardize_selected
        transaction_value_normalized_data = pd.concat([transaction_value_normalized_data, result_temp])
        
    return transaction_value_normalized_data

def combine_daily_csv_files_by_day(source_folder, output_folder, day):
    try:
        os.makedirs(output_folder, exist_ok=True)
        daily_dataframes = []  # To hold all DataFrames for the day
        for file in os.listdir(source_folder + day):
            if file.endswith(".csv"):
                file_path = os.path.join(source_folder + day, file)
    
                # Read the CSV file
                try:
                    df = pd.read_csv(file_path)
                    daily_dataframes.append(df)
                except Exception as e:
                    print(f"Failed to read {file_path}: {e}")
                    continue
    
        # Combine all event_id DataFrames for the day
        if daily_dataframes:
            combined_daily_df = pd.concat(daily_dataframes, ignore_index=True)
            output_path = os.path.join(output_folder, f"{day}.csv")
    
            # Save the combined daily CSV
            combined_daily_df.to_csv(output_path, index=False)
            print(f"Combined daily file saved for {source_folder}{day} at {output_path}")
            
    except Exception as e:
        print(f"Skipping non-directory: {e}")
        
    print("All daily files have been combined.")

def copy_csv_files(source_folder, destination_folder):
    """
    Copies all CSV files from the source folder to the destination folder.

    Parameters:
    source_folder (str): Path to the source folder containing the CSV files.
    destination_folder (str): Path to the destination folder where files will be copied.
    """
    # Ensure the destination folder exists
    os.makedirs(destination_folder, exist_ok=True)
    
    # Iterate through all files in the source folder
    for filename in os.listdir(source_folder):
        if filename.endswith('.csv'):  # Check if the file has a .csv extension
            source_path = os.path.join(source_folder, filename)
            destination_path = os.path.join(destination_folder, filename)
            shutil.copy(source_path, destination_path)
            
########################################
######### !!! WARNING timezone_offset !!!
# # Example usage

# start_time = 30
# end_time = 10500
# delta = 60
# min_delta = 10
# threshold_step = 80
# num_points = 300
# select_time_array = generate_smooth_time_steps(start_time, end_time, threshold_step, delta, min_delta, num_points)

# df = option_type_B_df
# start_def = last_available_flag_timestamp
# timezone_offset = 7200 ######### !!! WARNING timezone_offset !!!

# transaction_value_normalized_data = normalize_transaction_value_data(df, start_def, timezone_offset, start_time, end_time, select_time_array)
########################################  
# # Some operations on later usage !!!

# # Add event_id and entity_id
# transaction_value_normalized_data['event_id'] = event_id
# transaction_value_normalized_data['entity_id'] = transaction_value_normalized_data['num_service'].apply(
#     lambda num_service: entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].values[0] if not entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].empty else None
# )

# # Transform to transaction_value_ratios
# # Step 1: Calculate the sum of each column
# column_sums = transaction_value_normalized_data.sum()

# # Step 2: Divide each value in the DataFrame by the sum of its column
# df_normalized = transaction_value_normalized_data.div(column_sums, axis=1)

# # Step 3: Add a row with the sum of the columns (which will be 1 for normalized values)
# df_normalized.loc['Total'] = df_normalized.sum()
########################################

#%%
#scores_functions

def extract_diff(value):
    """Extracts diff from a string using predefined patterns."""
    if value == '#####':
        return 0.08
    elif value == '#####':
        return 0.1
    elif value == '#####':
        return 0.05
    elif value == '#####.':
        return 0.15
    elif value == '#####':
        return 0.25
    elif value == '#####':
        return 0.0
    elif value == '#####':
        return 40
    else:
        # Regular expression patterns
        match_single = re.search(r'(\d+(\.\d+)?)\s*L', value)
        match_frac = re.search(r'(\d+)/(\d+)\s*L', value)
        match_combined = re.search(r'(\d+(\.\d+)?)\s*L\s+(\d+)/(\d+)', value)

        if match_combined:
            whole = float(match_combined.group(1))
            numerator, denominator = map(int, (match_combined.group(3), match_combined.group(4)))
            return whole + numerator / denominator if denominator != 0 else whole
        elif match_frac:
            numerator, denominator = map(int, (match_frac.group(1), match_frac.group(2)))
            return numerator / denominator if denominator != 0 else np.nan
        elif match_single:
            return float(match_single.group(1))
        else:
            return np.nan

def process_sentences_data(text, day, FSC_placeholder):
    
    # Ensure 'text' is a string
    if not text or not isinstance(text, str):
        return []  # Return empty if text is None or not a string

    if text == "I":
        return np.nan

    # Compute the cutoff year
    cutoff_date = day - timedelta(days=360)
    cutoff_year = cutoff_date.year
    
    # Extract and check all years in parentheses
    for match in re.finditer(r'\((\d+)\)', text):
        extracted_year = int(match.group(1))

        # Convert 2-digit year to 4-digit
        extracted_year += 2000 if extracted_year < 50 else 1900

        if extracted_year < cutoff_year:
            text = text[:match.start()]  # Truncate string before the invalid year
            break  # Stop checking further
        
    # Step 1: Extract and temporarily ignore numbers inside parentheses (e.g., '(23)')
    text_with_placeholder = re.sub(r'\((\d+)\)', r'(__PARENTHESES__\1__PARENTHESES__)', text)
    
    # Replace numbers greater than 6 with '7'
    text_with_placeholder = re.sub(r'(?<!\d)([7-9]|[1-9][0-9]+)(?!\d)', '7', text_with_placeholder)
    
    # Step 3: Replace all standalone '0' with '8'
    text_with_placeholder = re.sub(r'(?<!\d)0(?!\d)', '8', text_with_placeholder)
        
    # Step 2: Replace 'D', 'T', 'A', 'J' with '9'
    text_with_placeholder = re.sub(r'[DTAJ]', (FSC_placeholder), text_with_placeholder)
    
    # Step 5: Restore numbers inside parentheses
    text = re.sub(r'__PARENTHESES__(\d+)__PARENTHESES__', r'(\1)', text_with_placeholder)
    
    # Remove anything inside parentheses
    text = re.sub(r'\(.*?\)', '', text)
    
    # category mapping: 
    category_map = {'i': 1, 't': 2, 'p': 3, 'j': 4, 'a': 5, 'x': 6}
    
    # Extract numbers and their corresponding letters
    matches = re.findall(r'(\d+)([ampshc])', text)
    
    # Convert to a list of (number, category_id) tuples
    extracted = [(int(num), category_map[letter]) for num, letter in matches]
    
    # Take the first 9 entries
    return extracted[:9]

# Define a function to expand processed tuples into columns
def expand_processed_data(processed_data, valid_range):
    expanded = {}

    # Convert Pandas Series or NumPy array to list
    if isinstance(processed_data, (pd.Series, np.ndarray)):  
        processed_data = processed_data.tolist()  

    # Handle NaN values or empty lists correctly
    if isinstance(processed_data, float) and pd.isna(processed_data):  # Scalar NaN case
        processed_data = []
    
    if not isinstance(processed_data, list) or len(processed_data) == 0:  # Handle non-list or empty list
        processed_data = [(0, 0)] * (valid_range - 1)  # Default values

    # Initialize dictionary with zero values
    for i in range(1, valid_range):
        expanded[f'm{i}'] = 0
        expanded[f'm{i}_d'] = 0

    # Populate dictionary with values from processed_data
    for i, (num, category) in enumerate(processed_data):
        if i < (valid_range - 1):  # Ensure index stays within valid range
            expanded[f'm{i + 1}'] = num
            expanded[f'm{i + 1}_d'] = category

    return expanded

def calculate_THL_diff(df):
    """Calculates cumulative THL difference by event."""
    df['THL_diff'] = df.groupby(['event_id'])['THL_value'].cumsum()
    return df

def sort_and_move_nans(row):
    # Filter out NaN values and sort the remaining values
    valid_values = ([x for x in row if not pd.isna(x)])
    # Count how many NaN values exist
    nan_count = row.isna().sum()
    # Add NaN values at the end of the row
    return valid_values + [np.nan] * nan_count

# Function to calculate ratio for each operator
def calculate_ratio(group, subject_id, mean_column):
    subject_mean = group[group['subject_id'] == subject_id][mean_column].values[0]
    other_means = group[group['subject_id'] != subject_id][mean_column].mean()
    return other_means / subject_mean if subject_mean != 0 else 0

def extract_valid_sentences(data, category_id, valid_range): 
    filtered_results = []
    
    # Iterate through the rows of the DataFrame
    for index, row in data.iterrows():
        # Create a list to hold the final results for the current row
        final_results = []
    
        # Iterate over the event results columns r1 to r9
        for i in range(1, valid_range):
            # Define the result column (rX) and the corresponding category column (rX_d)
            event_column = f'm{i}'
            category_column = f'm{i}_d'   
    
            # Check if the category matches the event's category_id
            if row[category_column] == category_id:
                # If it matches, add the result to the final_results
                final_results.append(row[event_column])
            else:
                # If it doesn't match, add NaN (or another placeholder) to indicate an invalid result
                final_results.append(None)  # Could also use `np.nan` here
    
        # Append the final results for the current row to the filtered_results list
        filtered_results.append(final_results)
    
    # Convert the filtered results to a DataFrame
    data = pd.DataFrame(filtered_results, columns=[f'm{i}' for i in range(1, valid_range)])    
    return data