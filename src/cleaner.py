#%% notes

# !!! add logger information !!!

#%% modules
import os
os.chdir(r'C:\Projekt\service\2_program\service_program')

from datetime import datetime
import pandas as pd
import numpy as np
import json
import re
import time

from glob import glob

import ast
import math

from my_functions import load_yaml, create_logging
from data_integration import resolver, fetch_data_from_db

config = load_yaml('config')
dictionary = load_yaml('dictionary')

# threshold for correcting rounding errors when calculate transaction_value_value_estimated
threshold = config['clean_config']['threshold']
transaction_value_threshold = config['clean_config']['transaction_value_threshold']
#%% functions

def build_dataframe_from_csvs(event_data, folder_path):
    """
    Build a DataFrame from the first rows of all CSV files matching a event identifier.
    
    Parameters:
    - event_id (dict): A dictionary with keys 'num_session' and 'num_event'.
    - folder_path (str): Path to the folder containing the CSV files.
    
    Returns:
    - pd.DataFrame: A DataFrame containing the first rows and a `timestamp` column.
    """
    # Create the event identifier string
    event_identifier = f"R{event_data['num_session'][0]}C{event_data['num_event'][0]}"
    
    # Gather all CSV files in the specified folder
    csv_files = glob(os.path.join(folder_path, "*.csv"))
    
    # List to store the rows and timestamps
    rows = []
    
    for file in csv_files:
        file_name = os.path.basename(file).strip()
        file_prefix = file_name.split('_')[0]
        # Check if the file name contains the event identifier
        if event_identifier == file_prefix:
            # Extract the timestamp from the file name
            match = re.search(r'_(\d+)', file_name)
            if match:
                timestamp = float(match.group(1))  # Convert timestamp to a float
                # Read only the first row of the CSV file
                try:
                    first_row = pd.read_csv(file, nrows=1)
                    first_row['timestamp'] = timestamp  # Add the timestamp column
                    rows.append(first_row)
                except Exception as e:
                    print(f"Error reading file {file}: {e}")
    # Combine all rows into a single DataFrame
    if rows:
        combined_df = pd.concat(rows, ignore_index=True)
        data = combined_df[['timestamp', 'available_flag']]    
        return data

def format_values(val):
    # Replace 'NP' with 99 and ensure all values are integers
    formatted_values = [99 if x == 'NP' else int(x) for x in val]
    
    # Convert the list to a PostgreSQL-compatible integer[] format
    return f"{{{','.join(map(str, formatted_values))}}}"
    
def populate_data_from_raw(data, raw_data, dictionary, category): # , table_name
    """
    Populates the 'data' DataFrame with columns from 'raw_data' using keys specified in 'dictionary'.

    Parameters:
    - data (pd.DataFrame): The DataFrame to populate.
    - raw_data (pd.DataFrame or dict): The raw data source with values.
    - dictionary (dict): A dictionary defining mappings for categories.
    - category (str): The key in the dictionary for the category to populate.
    """
    
    for column_name, raw_data_key in dictionary[category].items():
        try:
            # Populate the data DataFrame with the value from raw_data if raw_data_key exists and if it not fill with nan
            if raw_data_key in raw_data:
                data[column_name] = raw_data[raw_data_key]
            else:
                data[column_name] = np.nan
        except Exception as e:
            print(e)
            pass
            
    return data

class NameCleaner:
    """
    A utility class for normalizing and cleaning text columns 
    (e.g., removing punctuation, abbreviations, extra spaces, splitting names).
    """
    def __init__(self, column_name, multiple = 2):
        self.column_name = column_name
        self.multiple = multiple

    @staticmethod
    def add_spaces_to_text(text):
        return re.sub(r'(\d)([a-zA-Z])', r'\1 \2', re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text))

    @staticmethod
    def replace_comma_slash_and(text):
        text = re.sub(r'[,/&]', r' // ', text)
        return re.sub(r'\b(?:and|AND)\b', r' // ', text)

    @staticmethod
    def clean_name(name):
        parts = name.split('//')
        cleaned_parts = []
        for part in parts:
            words = part.split()
            cleaned_part = words[0][0] + ' ' + ' '.join(words[1:]) if len(words) > 1 else part
            cleaned_parts.append(cleaned_part)
        return ' // '.join(cleaned_parts)

    @staticmethod
    def clean_abbreviation_of_name(name):
        name_parts = name.split('//')
        cleaned_parts = []
        for part in name_parts:
            words = part.strip().split()
            words = [word for word in words if word.upper() not in [
                '''###############'''
            ]]
            cleaned_parts.append(' '.join(words))
        return ' // '.join(cleaned_parts)

    @staticmethod
    def remove_parentheses(name):
        return re.sub(r'\([^)]*\)', '', name)

    @staticmethod
    def remove_wrong_parentheses(name):
        cleaned_name = re.sub(r'\(.*?\s', '', name)
        return cleaned_name.strip()

    @staticmethod
    def remove_ampersand(text):
        pattern = r'\b[A-Za-z]&[A-Za-z]\b'
        return re.sub(pattern, '', text)

    @staticmethod
    def add_space_before_opening_parenthesis(text):
        return text.replace('(', ' (')

    @staticmethod
    def add_space_after_closing_parenthesis(text):
        return text.replace(')', ') ')

    @staticmethod
    def remove_lone_numbers(text):
        return re.sub(r'\b\d\b', '', text)

    @staticmethod
    def remove_double_slashes_at_start_and_end(text):
        text = re.sub(r'^//\s*', '', text)
        return re.sub(r'//\s*$', '', text)

    def clean_column(self, df, multiple):
        column = self.column_name
        df[column] = df[column].str.upper()
        df[column] = df[column].fillna('')
        df[column] = df[column].apply(self.add_space_before_opening_parenthesis)
        df[column] = df[column].apply(self.add_space_after_closing_parenthesis)
        df[column] = df[column].apply(self.remove_wrong_parentheses)
        df[column] = df[column].apply(self.remove_parentheses)
        df[column] = df[column].apply(self.remove_ampersand)
        df[column] = df[column].apply(self.clean_abbreviation_of_name)
        df[column] = df[column].apply(self.remove_lone_numbers)
        df[column] = df[column].apply(self.replace_comma_slash_and)
        df[column] = df[column].str.replace('[^a-zA-Z0-9// ]', ' ', regex=True)
        df[column] = df[column].str.replace(r'\s+', ' ', regex=True)
        df[column] = df[column].str.strip()
        df[column] = df[column].apply(self.add_spaces_to_text)
        df[column] = df[column].apply(lambda x: re.sub(r'^\d+\s*', '', x))
        df[column] = df[column].apply(self.remove_lone_numbers)
        df[column] = df[column].apply(self.remove_double_slashes_at_start_and_end)
        df[column] = df[column].apply(self.clean_name)
        df[column] = df[column].apply(self.remove_double_slashes_at_start_and_end)
        df[column] = df[column].str.strip()
        if multiple > 1:
            df['Name_split'] = df[column].str.split(' // ')
            split_df = df['Name_split'].apply(pd.Series)
            split_df.columns = [f'{column}{i+1}' for i in range(split_df.shape[1])]
            df = pd.concat([df, split_df], axis=1)
            df.drop(columns=['Name_split'], axis=1, inplace=True)
            for i in range(split_df.shape[1]):
                df[f'{column}{i+1}'] = df[f'{column}{i+1}'].str.strip()
            if split_df.shape[1] > 4:
                # Identify columns to drop dynamically
                columns_to_drop = [f'{column}{i+1}' for i in range(4, split_df.shape[1])]
                # Drop these columns if they exist in the DataFrame
                df.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1, inplace=True)
        return df
   
def custom_round_return_rate(x):
    if x < 10:
        if x / round(x, 1) < 1:
            return round(x - 0.1, 1)
        else:
            return round(x, 1)
    else:
        return int(x) 
def custom_round_transaction_value(x):
    if x > 0:
        y = x/100
        y = math.floor(y)
        return y * 100
    else:
        return int(x)     
def custom_round_return_rate_final(x):
    if x > 0:
        return round(x, 1)
    else:
        return int(x)

#%%
    
class RealValueFiller:
    """
    A utility to merge multiple transaction datasets and ensure consistency
    across time and services. It fills missing operators, normalizes ratios,
    and removes invalid entries.

    Typical use case:
        df1 = ...
        df2 = ...
        base = ...
        filler = RealValueFiller(df1, df2, base)
        cleaned_df, dropped_count, non_active_count = filler.process()
    """    
    def __init__(self, df1, df2, base_data):
        self.df1 = df1
        self.df2 = df2
        self.base_data = base_data

    def merge_dataframes(self):
        merged_df = pd.merge(self.df1, self.df2, on=['update_time', 'num_service'], how='outer')
        merged_df['sum_total_transaction_value'] = merged_df['sum_total_transaction_value_x'].where(pd.notna(merged_df['sum_total_transaction_value_x']), np.nan)
        merged_df = merged_df.drop(columns=['sum_total_transaction_value_y', 'sum_total_transaction_value_x'])
        return merged_df

    def handle_missing_num_service(self, df):
        """
        Ensure all num_service are present for each timestamp using base_data.
        
        Args:
            df: Merged DataFrame with event data.
            base_data: DataFrame containing 'num_service' and 'non_active_entry'.
        
        Returns:
            DataFrame with all operators filled for each timestamp.
        """
        grouped_sums = df.groupby('update_time')['return_rate_value'].transform('sum')
        df['raw_return_rate_ratio'] = df['return_rate_value'] / grouped_sums
        
        filled_data = []
        # Get the full list of operators who are not non_active_entry
        all_num_service = self.base_data[~self.base_data['non_active_entry']]['num_service'].tolist()
        for update_time, group in df.groupby('update_time'):
            # Existing num_service in this timestamp
            existing_num_service = set(group['num_service'].dropna())
            # num_service missing for this timestamp but present in all_num_service
            missing_num_service = set(all_num_service) - existing_num_service
            
            # inser missing num_service
            if len(missing_num_service) > 0:
                for num_service in missing_num_service:
                    filled_data.append({
                        'update_time': update_time,
                        'num_service': num_service,
                        'transaction_value_ratio': np.nan,
                        'return_rate': np.nan,
                        'return_rate_value': np.nan,
                        'sum_total_transaction_value': group['sum_total_transaction_value'].iloc[0]
                    })
        
        filled_df = pd.DataFrame(filled_data)
        df = pd.concat([df, filled_df], ignore_index=True)
        
        non_nan_sum_total_transaction_value = (
            df.groupby('update_time')['sum_total_transaction_value']
            .apply(lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan)
        )
        
        # Fill NaN values in 'sum_total_transaction_value' with the corresponding group value
        df['sum_total_transaction_value'] = df.apply(
            lambda row: non_nan_sum_total_transaction_value[row['update_time']] if pd.isna(row['sum_total_transaction_value']) else row['sum_total_transaction_value'],
            axis=1
        )
        # Count Filter out rows where 'num_service' is 'non_active_entry'
        x = 0
        list_non_active_entry_founded = []
        for index, row in df.iterrows():
            if row['num_service'] not in all_num_service:
                list_non_active_entry_founded.append(row['num_service'])
                # print('non_active_entry founded!!!!!!!!!!!!!!!!!', row['num_service'])
                df.drop(index, inplace=True)
                x += 1
        unique_non_active_entry_count = len(set(list_non_active_entry_founded))
                
        # Reindex the DataFrame by 'update_time' and 'num'
        df = df.set_index(['update_time', 'num_service']).sort_index().reset_index()   
        
        grouped_sums = df.groupby('update_time')['return_rate_value'].transform('sum')
        df['return_rate_transaction_value_ratio_estimated'] = df['return_rate_value'] / grouped_sums
        
        return df, x, unique_non_active_entry_count


    def process(self):
        merged_df = self.merge_dataframes()
        merged_df, x, unique_non_active_entry_count = self.handle_missing_num_service(merged_df)
        return merged_df, x, unique_non_active_entry_count

def find_transaction_value_ratio_estimated(df):
    """ """
    interpolated_df = df[['update_time', 'num_service', 'transaction_value_ratio', 'return_rate_transaction_value_ratio_estimated']].copy()
    interpolated_df['interpolated_transaction_value_ratio'] = interpolated_df.apply(
        lambda row: row['transaction_value_ratio'] if not pd.isna(row['transaction_value_ratio']) else row['return_rate_transaction_value_ratio_estimated'],
        axis=1
    )

    interpolated_df_sorted = interpolated_df.sort_values(by=['update_time', 'num_service']).reset_index(drop=True)

    interpolated_df_sorted['interpolated_transaction_value_ratio'] = (
        interpolated_df_sorted.groupby('num_service')['interpolated_transaction_value_ratio']
        .transform(lambda x: x.interpolate(method='linear', limit_direction='both'))
    )
    
    df = df.merge(interpolated_df_sorted[['update_time', 'num_service', 'interpolated_transaction_value_ratio']], on=['update_time', 'num_service'], how='left')
    df['sum_total_transaction_value_ratio'] = df.groupby('update_time')['transaction_value_ratio'].transform('sum')
    df['sum_total_interpolated_transaction_value_ratio'] = df.groupby('update_time')['interpolated_transaction_value_ratio'].transform('sum')
    
    df['interpolated_transaction_value_ratio'] = df.apply(
        lambda row: row['interpolated_transaction_value_ratio'] * row['sum_total_interpolated_transaction_value_ratio']
        if (0.99 <= row['sum_total_interpolated_transaction_value_ratio'] <= 1.01)
        else row['interpolated_transaction_value_ratio'] * (row['sum_total_transaction_value_ratio'] / row['sum_total_interpolated_transaction_value_ratio']),
        axis=1
    )
    # df['interpolated_transaction_value_ratio'] = df['interpolated_transaction_value_ratio'] * (df['sum_total_transaction_value_ratio'] / df['sum_total_interpolated_transaction_value_ratio'])
    df['sum_total_return_rate_transaction_value_ratio_estimated'] = df.groupby('update_time')['return_rate_transaction_value_ratio_estimated'].transform('sum')    
    df['return_rate_transaction_value_ratio_estimated'] = df['return_rate_transaction_value_ratio_estimated'] * df['sum_total_transaction_value_ratio'] #/ df['sum_total_return_rate_transaction_value_ratio_estimated'])
    
    return df

def handle_missing_transaction_value_ratio(df):
    for update_time, group in df.groupby('update_time'):
        # Find missing 'transaction_value_ratio' rows
        missing_rows = group[group['transaction_value_ratio'].isna()]

        if missing_rows.shape[0] > 0:
            for i in range(len(missing_rows)):
                missing_num_service = missing_rows['num_service'].iloc[i]
                missing_transaction_value_ratio = missing_rows['interpolated_transaction_value_ratio']
                # Update the missing 'transaction_value_ratio' directly in the main DataFrame
                df.loc[(df['update_time'] == update_time) & (df['num_service'] == missing_num_service), 'transaction_value_ratio'] = missing_transaction_value_ratio
                
    df['transaction_value_value_estimated'] = df['transaction_value_ratio'] * df['sum_total_transaction_value']
    return df

def filter_all_nan_transaction_value_values(df):
    """
    Filters out groups where all 'transaction_value_value' entries are NaN and returns two DataFrames:
    - One with valid groups.
    - Another with groups that have all NaN 'transaction_value_value' entries.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
    
    Returns:
        valid_df (pd.DataFrame): DataFrame with non-all-NaN 'transaction_value_value' groups.
        nan_groups_df (pd.DataFrame): DataFrame with groups where all 'transaction_value_value' are NaN.
    """
    nan_groups = []
    valid_groups = []
    
    for update_time, group in df.groupby('update_time'):
        if group['transaction_value_value'].isna().all():
            nan_groups.append(group)
        else:
            valid_groups.append(group)

    # Concatenate filtered DataFrames
    valid_df = pd.concat(valid_groups, axis=0).reset_index(drop=True)
    
    return valid_df

def drop_update_times_with_decreasing_sum_total(df, threshold=0):
    """
    Drop rows where 'sum_total_transaction_value_estimated' is less than the previous 'sum_total_transaction_value_estimated' for each 'update_time' and
    count how many 'update_time' rows are dropped. The process repeats until no more rows are dropped.

    Parameters:
    - df (pd.DataFrame): The input DataFrame with 'sum_total_transaction_value_estimated' and 'update_time'.
    
    Returns:
    - df (pd.DataFrame): DataFrame with the dropped rows where 'sum_total_transaction_value_estimated' was decreasing.
    - dropped_count (int): The count of how many 'update_time' rows were dropped.
    """
    dropped_update_time_count = 0

    while True:
        # Sort by 'update_time' to ensure proper ordering of rows
        df = df.sort_values(by=['update_time']).reset_index(drop=True)

        # Calculate the difference in 'sum_total' between consecutive 'update_time' values
        df['sum_total_transaction_value_estimated_diff'] = df['sum_total_transaction_value_estimated'].diff()

        # Identify all update_time rows where 'sum_total' is less than the previous 'sum_total'
        decreasing_update_times = df[df['sum_total_transaction_value_estimated_diff'] < threshold]['update_time'].unique()

        # If no rows are found where 'sum_total' decreased, break out of the loop
        if len(decreasing_update_times) == 0:
            break

        # Drop rows belonging to these 'update_time' groups
        df = df[~df['update_time'].isin(decreasing_update_times)].drop(columns=['sum_total_transaction_value_estimated_diff'])

        # Count how many update_time rows were dropped
        dropped_update_time_count += len(decreasing_update_times)

    return df, dropped_update_time_count

def correct_small_decreases(df):
    """
    Corrects small decreases in 'transaction_value_value_estimated' by setting them equal to the previous 'transaction_value_value_estimated'.
    
    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - threshold (int): The threshold for small decreases (negative values in transaction_value_value_estimated_diff).
    
    Returns:
    - df (pd.DataFrame): DataFrame with corrected small decreases.
    """
    # Sort by 'num' and 'update_time' to ensure proper ordering
    df = df.sort_values(by=['num_service', 'update_time']).reset_index(drop=True)

    # Calculate the difference of 'value' for each 'num'
    df['transaction_value_value_estimated_diff'] = df.groupby('num_service')['transaction_value_value_estimated'].diff()

    # Correct small decreases within the threshold (threshold is absolute value)
    small_decreases = (df['transaction_value_value_estimated_diff'] < 0) & (df['transaction_value_value_estimated_diff'] >= -df['transaction_value_threshold'])
    
    # Set the current value to the previous value where small decreases occur
    # Use the previous non-NaN value in the same 'num' group to avoid introducing NaNs
    df.loc[small_decreases, 'transaction_value_value_estimated'] = df.groupby('num_service')['transaction_value_value_estimated'].shift(1)
    
    # Drop the 'value_diff' column
    # df = df.drop(columns=['transaction_value_value_estimated_diff'])
    
    return df

def correct_small_decreases_until_no_more(df, threshold=500):
    """
    Corrects small decreases in 'transaction_value_value_estimated' iteratively by setting them equal to the previous 'transaction_value_value_estimated'.
    Continues iterating until no small decreases are found below the threshold.
    Also counts the number of small decreases corrected during each iteration.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - threshold (int): The threshold for small decreases (negative values in transaction_value_value_estimated_diff).

    Returns:
    - df (pd.DataFrame): DataFrame with corrected small decreases.
    - total_corrections (int): Total number of corrections made to small decreases.
    """
    # Sort by 'num' and 'update_time' to ensure proper ordering
    df = df.sort_values(by=['num_service', 'update_time']).reset_index(drop=True)
    
    # calculate the transaction_value_threshold to compare proportionally
    df['transaction_value_threshold'] = df['sum_total_transaction_value_estimated'] / transaction_value_threshold * df['transaction_value_ratio_estimated'] * threshold
    
    # Initialize a flag to check if there are small decreases to correct
    small_decreases_exist = True
    total_corrections = 0  # Initialize the counter for small decreases corrected

    while small_decreases_exist:
        # Apply the correction function
        df = correct_small_decreases(df)

        # Calculate the difference of 'transaction_value_value' for each 'num_service' again
        df['transaction_value_value_estimated_diff'] = df.groupby('num_service')['transaction_value_value_estimated'].diff()
        
        # Identify small decreases again
        small_decreases = (df['transaction_value_value_estimated_diff'] < 0) & (df['transaction_value_value_estimated_diff'] >= -df['transaction_value_threshold'])

        # Count the number of small decreases corrected in this iteration
        corrections_in_iteration = small_decreases.sum()
        total_corrections += corrections_in_iteration

        # Check if there are still any small decreases
        small_decreases_exist = small_decreases.any()

        # Drop the 'value_diff' column as we don't need it anymore
        # df = df.drop(columns=['transaction_value_value_estimated_diff'])

    # print(f"Total small decreases corrected: {total_corrections}")
    return df, total_corrections

############ this function is not using but interesting for later usage !!!! ############
def correct_small_decreases_until_positive(df, threshold=500):
    """
    Corrects small decreases in 'transaction_value_value_estimated' by reducing the negative difference amount across all previous rows
    until the current 'value' is no longer less than the previous 'transaction_value_value_estimated'.
    
    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - threshold (int): The threshold for small decreases to correct.
    
    Returns:
    - df (pd.DataFrame): DataFrame with corrected small decreases.
    """
    # Sort by 'num' and 'update_time' to ensure proper ordering
    df = df.sort_values(by=['num_service', 'update_time']).reset_index(drop=True)
    
    # calculate the transaction_value_threshold to compare proportionally
    df['transaction_value_threshold'] = df['sum_total_transaction_value_estimated'] / transaction_value_threshold * df['transaction_value_ratio_estimated']

    # Calculate the difference of 'value' for each 'num'
    df['transaction_value_value_estimated_diff'] = df.groupby('num_service')['transaction_value_value_estimated'].diff()

    for idx in df[df['transaction_value_value_estimated_diff'] < 0].index:
        # Get the negative difference for this row
        negative_diff = abs(df.loc[idx, 'transaction_value_value_estimated_diff'])

        # Identify previous rows in the same 'num_service' group
        group = df[df['num_service'] == df.loc[idx, 'num_service']]
        previous_rows = group[group['update_time'] < df.loc[idx, 'update_time']]

        # Remove the negative amount from previous rows until it is fully annulled
        if not previous_rows.empty:
            for prev_idx in previous_rows.index[::-1]:  # Start from the most recent previous row
                if negative_diff <= 0:
                    break
                available_to_remove = min(df.loc[prev_idx, 'transaction_value_value_estimated'] - threshold, negative_diff)
                df.loc[prev_idx, 'transaction_value_value_estimated'] -= available_to_remove
                negative_diff -= available_to_remove

            # Ensure the current row's value is no longer less than the corrected previous value
            df.loc[idx, 'transaction_value_value_estimated'] = max(df.loc[idx, 'transaction_value_value_estimated'], df.loc[idx - 1, 'transaction_value_value_estimated'])

    # Drop the 'value_diff' column as it is no longer needed
    df = df.drop(columns=['transaction_value_value_estimated_diff'])

    return df
####################################################################################

def reducing_update_time_with_decreasing_num_values(df):
    df_1 = df.copy()
    # print(selected_place.groupby('num_service')['transaction_value_value_estimated'].apply(lambda x: x.is_monotonic_increasing))
    dropped_update_times_count = 0
    df_1 = df_1.sort_values(by=['num_service', 'update_time']).reset_index(drop=True)
    
    while True:
        # Calculate the difference of 'value' for each 'num'
        df_1['transaction_value_value_estimated_diff'] = df_1.groupby('num_service')['transaction_value_value_estimated'].diff()
        
        # Identify all `update_time` groups where any 'value_diff' is negative
        decreasing_update_times = df_1[df_1['transaction_value_value_estimated_diff'] < 0]['update_time'].unique()
        
        # If no decreasing update_time found, break the loop
        if not decreasing_update_times.size:
            break
        
        # Count the number of dropped update_time groups
        dropped_update_times_count += len(decreasing_update_times)
        
        # Drop rows belonging to these `update_time` groups
        df_1 = df_1[~df_1['update_time'].isin(decreasing_update_times)].drop(columns=['transaction_value_value_estimated_diff'])

    return dropped_update_times_count                


#%%

# transaction_value_raw = transaction_value_type_B_data
# return_rate_raw = return_rate_data
# transaction_value_place_data = transaction_value_place_data

# # exemple of starting work on couple_type_B_data
# transaction_value_couple_type_B_data = data.loc[data['operation_type_libelle_id'] == 3].copy()

# event_id = 12
# selected_couple_type_B = transaction_value_couple_type_B_data.loc[transaction_value_couple_type_B_data['event_id'] == event_id]

# selected_couple_type_B = selected_couple_type_B.copy()
# selected_couple_type_B['num_service_1'] = selected_couple_type_B['combinaison'].str.extract(r'\{(\d+),').astype(int)
# selected_couple_type_B['num_service_2'] = selected_couple_type_B['combinaison'].str.extract(r'\,(\d+)\}').astype(int)

# # Step 1: Drop duplicate combinations of 'num_1' and 'num_2', keeping the one with the highest 'value'
# unique_combinations = selected_couple_type_B.sort_values('transaction_value_value', ascending=False).drop_duplicates(subset=['num_service_1', 'num_service_2'])

# # Step 2: Select the top 20 combinations based on the highest 'value'
# top_combinations = unique_combinations.nlargest(8, 'transaction_value_value')[['num_service_1', 'num_service_2']]
# filtered_data = selected_couple_type_B.merge(top_combinations, on=['num_service_1', 'num_service_2'], how='inner')

# filtered_data = filtered_data.drop(columns=['day', 'event_id', 'num_event', 'num_session', 'operation_type_libelle_id', 'combinaison', 'collected_at', 'operation_type_libelle'])


def transaction_value_mod(day_db_format, transaction_value_raw, return_rate_raw, transaction_value_place_data):
    logger = create_logging(config['log_stages']['clean'])
    try:
        event_list = transaction_value_raw['event_id'].unique()
        column_names = dictionary['event_info_transaction_value_data']['columns']
    
        # Initialize an empty list to hold rows
        rows = []
        option_type_B_data = pd.DataFrame([])
        option_type_A_data = pd.DataFrame([])
        
        for event_id in event_list:
            #try:
                #################
                # ....
                #################
            rows, option_type_B_data, option_type_A_data = transaction_value_mod_event(event_id, transaction_value_raw, return_rate_raw, transaction_value_place_data, day_db_format, rows, option_type_B_data, option_type_A_data, logger)
            #################
        event_info_data = pd.DataFrame(columns=column_names)
        for row in rows:
            event_info_data.loc[len(event_info_data)] = row
        event_info_data['event_info_transaction_value_data_id'] = np.nan
    
        logger.info(f"transaction_value_mod and event_info data from day {day_db_format} successfully prepared.")
                  
        return event_info_data, option_type_B_data, option_type_A_data

    except Exception as e:
        logger.error(f"An error occurred: {e} in transaction_value_mod")
        return None, None, None
    
def transaction_value_mod_event(event_id, transaction_value_raw, return_rate_raw, transaction_value_place_data, day_db_format, rows, option_type_B_data, option_type_A_data, logger):
    try:
        # Generate the row for each event
        row = []
        query = f"SELECT * FROM entity WHERE event_id = {event_id}"
        entity_data = fetch_data_from_db(query, config)
        
        query = f"SELECT * FROM event WHERE event_id = {event_id}"
        event_data = fetch_data_from_db(query, config)
        
        selected_transaction_value = transaction_value_raw.loc[transaction_value_raw['event_id'] == event_id]
        selected_return_rate = return_rate_raw.loc[return_rate_raw['event_id'] == event_id]
        
        operation_type_libelle_id = transaction_value_raw.loc[transaction_value_raw['event_id'] == event_id, 'operation_type_libelle_id'].iloc[0]
        start_time = event_data['event_start_time'][0]
        available_flag_data = build_dataframe_from_csvs(event_data, config['db_file_directories']['base_target'] + config['db_file_directories']['available_flag'] + day_db_format)
        if available_flag_data is not None:
            try:
                last_available_flag_timestamp = available_flag_data.loc[available_flag_data['available_flag'] == True, 'timestamp'].max()
                if last_available_flag_timestamp is not None:
                    last_available_flag_timestamp = datetime.fromtimestamp(last_available_flag_timestamp)
                else:
                    logger.warning(f"no valid last_available_flag_timestamp for event_id {event_id} in transaction_value_mod")
                    last_available_flag_timestamp = start_time
            except Exception:
                logger.warning(f"no valid last_available_flag_timestamp for event_id {event_id} in transaction_value_mod")
                last_available_flag_timestamp = start_time
            start_time_reel = available_flag_data.loc[available_flag_data['available_flag'] == False, 'timestamp'].min()
            
            if not np.isnan(start_time_reel):
                start_time_reel = datetime.fromtimestamp(start_time_reel)
                unique_available_flag_count = len(available_flag_data)
    
                nombre_declares_active_entrys = entity_data.shape[0]
                nombre_active_entrys = nombre_declares_active_entrys - entity_data['non_active_entry'].sum()
                
                quality_score_list = entity_data.apply(lambda x: 1 / x['metric_A_return_rate'] if x['metric_A_return_rate'] > 0 and not np.isnan(x['metric_A_return_rate']) and x['non_active_entry'] == False else np.nan, axis=1)
                quality_score_metric_A_return_rate = quality_score_list.sum()
                
                unique_transaction_value_count = selected_transaction_value['update_time'].nunique()
                unique_return_rate_count = selected_return_rate['update_time'].nunique()
                
                selected_transaction_value = selected_transaction_value.copy()
                selected_transaction_value['num_service'] = selected_transaction_value['combinaison'].str.extract(r'\{(\d+)\}').astype(int)
                merge_df = pd.merge(selected_transaction_value, selected_return_rate, on = ['update_time', 'num_service'], how='outer')
                unique_timestamps = merge_df['update_time'].nunique()
                
                update_times = pd.concat([selected_transaction_value['update_time'], selected_return_rate['update_time']])
                update_time_list = update_times.unique().tolist()
                
                interval_1 = sum(last_available_flag_timestamp - pd.Timedelta(seconds=10800) <= value < last_available_flag_timestamp - pd.Timedelta(seconds=1800) for value in update_time_list)
                interval_2 = sum(last_available_flag_timestamp - pd.Timedelta(seconds=1800) <= value < last_available_flag_timestamp - pd.Timedelta(seconds=900) for value in update_time_list)
                interval_3 = sum(last_available_flag_timestamp - pd.Timedelta(seconds=900) <= value < last_available_flag_timestamp - pd.Timedelta(seconds=60) for value in update_time_list)
                interval_4 = sum(last_available_flag_timestamp - pd.Timedelta(seconds=60) <= value < last_available_flag_timestamp + pd.Timedelta(seconds=720) for value in update_time_list)
                
                selected_return_rate = return_rate_raw.loc[return_rate_raw['event_id'] == event_id].copy()
                selected_return_rate.loc[:, 'num_service'] = selected_return_rate['num_service'].astype(int)
                
                selected_transaction_value = selected_transaction_value.drop(columns=['day', 'event_id', 'num_event', 'num_session', 'operation_type_libelle_id', 'combinaison', 'collected_at', 'operation_type_libelle'])
                selected_return_rate = selected_return_rate.drop(columns=['day', 'event_id', 'num_session', 'num_event', 'collected_at'])
                
                selected_transaction_value = selected_transaction_value.sort_values(by=['update_time', 'num_service', 'sum_total_transaction_value'], ascending=[True, True, False])
                selected_transaction_value = selected_transaction_value.drop_duplicates(subset=['update_time', 'num_service'], keep='first')
                selected_return_rate = selected_return_rate.sort_values(by=['update_time', 'num_service', 'sum_total_transaction_value'], ascending=[True, True, False])
                selected_return_rate = selected_return_rate.drop_duplicates(subset=['update_time', 'num_service'], keep='first')
                
                selected_transaction_value.loc[:, 'transaction_value_ratio'] = selected_transaction_value['transaction_value_value'] / selected_transaction_value['sum_total_transaction_value']
                selected_return_rate.loc[:, 'return_rate_value'] = 1 / selected_return_rate['return_rate']
                grouped_sums = selected_return_rate.groupby('update_time')['return_rate_value'].transform('sum')
                selected_return_rate['return_rate_transaction_value_ratio_estimated'] = selected_return_rate['return_rate_value'] / grouped_sums
                
                option_type_A_df = transaction_value_place_data.loc[transaction_value_place_data['event_id'] == event_id]
                option_type_A_df = option_type_A_df.copy()
                option_type_A_df['num_service'] = option_type_A_df['combinaison'].str.extract(r'\{(\d+)\}').astype(int)
                option_type_A_df = option_type_A_df.drop(columns=['day', 'event_id', 'num_event', 'num_session', 'operation_type_libelle_id', 'combinaison', 'collected_at', 'operation_type_libelle'])
                option_type_A_df = option_type_A_df.sort_values(by=['update_time', 'num_service', 'sum_total_transaction_value'], ascending=[True, True, False])
                option_type_A_df = option_type_A_df.drop_duplicates(subset=['update_time', 'num_service'], keep='first')
                option_type_A_df.loc[:, 'transaction_value_ratio'] = option_type_A_df['transaction_value_value'] / option_type_A_df['sum_total_transaction_value']
                
                filled_data = []
                all_num_service = entity_data[~entity_data['non_active_entry']]['num_service'].tolist()  
                
                for update_time, group in option_type_A_df.groupby('update_time'):
                    existing_num_service = set(group['num_service'].dropna())
                    missing_num_service = set(all_num_service) - existing_num_service
                    
                    # inser missing num_service
                    if len(missing_num_service) > 0:
                        for num_service in missing_num_service:
                            filled_data.append({
                                'update_time': update_time,
                                'num_service': num_service,
                                'transaction_value_value': np.nan,
                                'sum_total_transaction_value': group['sum_total_transaction_value'].iloc[0]
                            })
                
                filled_df = pd.DataFrame(filled_data)
                option_type_A_df = pd.concat([option_type_A_df, filled_df], ignore_index=True)
    
                for index, i in option_type_A_df.iterrows():
                    if i['num_service'] not in all_num_service:
                        option_type_A_df.drop(index, inplace=True)
                
                processor = RealValueFiller(selected_transaction_value, selected_return_rate, entity_data)
                option_type_B_df, non_active_entry_count, unique_non_active_entry_count = processor.process()
                
                option_type_B_df = find_transaction_value_ratio_estimated(option_type_B_df)
                
                option_type_B_df = filter_all_nan_transaction_value_values(option_type_B_df)
    
                option_type_B_df = handle_missing_transaction_value_ratio(option_type_B_df)
                
                option_type_B_df['transaction_value_value_estimated'] = option_type_B_df['transaction_value_ratio'] * option_type_B_df['sum_total_transaction_value']
    
                option_type_B_df['transaction_value_value_estimated'] = (option_type_B_df['transaction_value_value_estimated'] / 100).round(0)
                option_type_B_df['sum_total_transaction_value_estimated'] = option_type_B_df.groupby('update_time')['transaction_value_value_estimated'].transform('sum')
                option_type_B_df['transaction_value_ratio_estimated'] = option_type_B_df['transaction_value_value_estimated'] / option_type_B_df['sum_total_transaction_value_estimated']
    
                option_type_B_df, option_type_B_dropped_update_time_count = drop_update_times_with_decreasing_sum_total(option_type_B_df, 0)
                option_type_B_df, option_type_B_total_corrections = correct_small_decreases_until_no_more(option_type_B_df, threshold)
                
                option_type_B_df['sum_total_transaction_value'] = (option_type_B_df['sum_total_transaction_value'] / 100).round(0)
                option_type_B_df['sum_total_transaction_value_estimated'] = option_type_B_df.groupby('update_time')['transaction_value_value_estimated'].transform('sum')
                option_type_B_df['sum_total_transaction_value_difference'] = option_type_B_df['sum_total_transaction_value_estimated'] - option_type_B_df['sum_total_transaction_value']
                option_type_B_df['transaction_value_ratio_estimated'] = option_type_B_df['transaction_value_value_estimated'] / option_type_B_df['sum_total_transaction_value_estimated']
                # option_type_B_df['transaction_value_ratio_estimated'] = option_type_B_df.groupby('update_time')['transaction_value_ratio_estimated'].transform('sum')
    
                option_type_B_reducing_update_times_count = reducing_update_time_with_decreasing_num_values(option_type_B_df)
    
                option_type_B_df['event_id'] = event_id
                
                option_type_B_df['entity_id'] = option_type_B_df['num_service'].apply(
                    lambda num_service: entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].values[0] if not entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].empty else None
                )
                
                option_type_B_ratio_estimated = option_type_B_df[['update_time', 'num_service', 'interpolated_transaction_value_ratio']]
                
                # Tolerance in seconds (e.g., ±30 seconds)
                tolerance = pd.Timedelta(seconds=10)
                
                # Function to find the closest match within the tolerance
                def find_closest_match(target_row):
                    close_matches = option_type_B_ratio_estimated[
                        (option_type_B_ratio_estimated['num_service'] == target_row['num_service']) &
                        (np.abs(option_type_B_ratio_estimated['update_time'] - target_row['update_time']) <= tolerance)
                    ]
                    if not close_matches.empty:
                        # Return the closest value_to_transfer
                        return close_matches.iloc[0]['interpolated_transaction_value_ratio']
                    return np.nan
                
                # Apply the function row by row
                option_type_A_df['interpolated_transaction_value_ratio'] = option_type_A_df.apply(find_closest_match, axis=1)
                
                option_type_A_df = option_type_A_df.sort_values(by=['update_time', 'num_service']).reset_index(drop=True)
    
                option_type_A_df['interpolated_transaction_value_ratio'] = (
                    option_type_A_df.groupby('num_service')['interpolated_transaction_value_ratio']
                    .transform(lambda x: x.interpolate(method='linear', limit_direction='both'))
                )
                
                option_type_A_df['sum_total_transaction_value_ratio'] = option_type_A_df.groupby('update_time')['transaction_value_ratio'].transform('sum')
                option_type_A_df['sum_total_transaction_value_ratio_difference'] = 1 - option_type_A_df['sum_total_transaction_value_ratio']
                
                for update_time, group in option_type_A_df.groupby('update_time'):
                    # Find missing 'transaction_value_ratio' rows
                    missing_rows = group[group['transaction_value_ratio'].isna()]
                    if missing_rows.shape[0] > 0:
                        missing_sum_rows = missing_rows['interpolated_transaction_value_ratio'].sum()
                        for i in range(len(missing_rows)):
                            missing_num_service = missing_rows['num_service'].iloc[i]
                            missing_transaction_value_ratio = missing_rows['sum_total_transaction_value_ratio_difference'] * missing_rows['interpolated_transaction_value_ratio'] / missing_sum_rows
                            option_type_A_df.loc[(option_type_A_df['update_time'] == update_time) & (option_type_A_df['num_service'] == missing_num_service), 'transaction_value_ratio'] = missing_transaction_value_ratio
      
                option_type_A_df['transaction_value_value_estimated'] = option_type_A_df['transaction_value_ratio'] * option_type_A_df['sum_total_transaction_value']
    
                option_type_A_df['transaction_value_value_estimated'] = (option_type_A_df['transaction_value_value_estimated'] / 100).round(0)
                option_type_A_df['sum_total_transaction_value_estimated'] = option_type_A_df.groupby('update_time')['transaction_value_value_estimated'].transform('sum')
                option_type_A_df['transaction_value_ratio_estimated'] = option_type_A_df['transaction_value_value_estimated'] / option_type_A_df['sum_total_transaction_value_estimated']
    
                option_type_A_df, option_type_A_dropped_update_time_count = drop_update_times_with_decreasing_sum_total(option_type_A_df, 0)
                option_type_A_df, option_type_A_total_corrections = correct_small_decreases_until_no_more(option_type_A_df, threshold)
                
                option_type_A_df['sum_total_transaction_value'] = (option_type_A_df['sum_total_transaction_value'] / 100).round(0)
                option_type_A_df['sum_total_transaction_value_estimated'] = option_type_A_df.groupby('update_time')['transaction_value_value_estimated'].transform('sum')
                option_type_A_df['sum_total_transaction_value_difference'] = option_type_A_df['sum_total_transaction_value_estimated'] - option_type_A_df['sum_total_transaction_value']
                option_type_A_df['transaction_value_ratio_estimated'] = option_type_A_df['transaction_value_value_estimated'] / option_type_A_df['sum_total_transaction_value_estimated']
                # option_type_A_df['transaction_value_ratio_estimated'] = option_type_A_df.groupby('update_time')['transaction_value_ratio_estimated'].transform('sum')
    
                option_type_A_reducing_update_times_count = reducing_update_time_with_decreasing_num_values(option_type_A_df)
    
                option_type_A_df['event_id'] = event_id
                
                option_type_A_df['entity_id'] = option_type_A_df['num_service'].apply(
                    lambda num_service: entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].values[0] if not entity_data.loc[entity_data['num_service'] == num_service, 'entity_id'].empty else None
                )
    
                option_type_B_sum_difference_ratio = abs(option_type_B_df['sum_total_transaction_value_difference']).mean() / option_type_B_df['sum_total_transaction_value_estimated'].mean()
                option_type_B_sum_total_transaction_value = option_type_B_df['sum_total_transaction_value_estimated'].max()
                
                option_type_A_sum_difference_ratio = abs(option_type_A_df['sum_total_transaction_value_difference']).mean() / option_type_A_df['sum_total_transaction_value_estimated'].mean()
                option_type_A_sum_total_transaction_value = option_type_A_df['sum_total_transaction_value_estimated'].max()
    
                option_type_B_unique_update_time_count = option_type_B_df['update_time'].nunique()
                option_type_A_unique_update_time_count = option_type_A_df['update_time'].nunique()
                
                option_type_B_sum_difference = option_type_B_df['sum_total_transaction_value_difference'].iloc[-1]
                option_type_A_sum_difference = option_type_A_df['sum_total_transaction_value_difference'].iloc[-1]
                
                option_type_B_data_temp = option_type_B_df[['event_id', 'entity_id', 'update_time', 'num_service', 'transaction_value_value_estimated', 'transaction_value_ratio_estimated']].copy()
                option_type_B_data_temp['transaction_value_option_type_B_mod_id'] = np.nan
                
                option_type_A_data_temp = option_type_A_df[['event_id', 'entity_id', 'update_time', 'num_service', 'transaction_value_value_estimated', 'transaction_value_ratio_estimated']].copy()
                option_type_A_data_temp['transaction_value_option_type_A_mod_id'] = np.nan
                
                option_type_B_data_temp['transaction_value_value_estimated'] = option_type_B_data_temp['transaction_value_value_estimated'].fillna(0)
                option_type_A_data_temp['transaction_value_value_estimated'] = option_type_A_data_temp['transaction_value_value_estimated'].fillna(0)
                
                option_type_B_data_temp['transaction_value_ratio_estimated'] = option_type_B_data_temp['transaction_value_ratio_estimated'].fillna(0)
                option_type_A_data_temp['transaction_value_ratio_estimated'] = option_type_A_data_temp['transaction_value_ratio_estimated'].fillna(0)
                
                first_update_timestamp = option_type_B_df['update_time'].min()
                
                row.append(event_id)
                row.append(operation_type_libelle_id)
                row.append(start_time)
                row.append(start_time_reel)
                row.append(last_available_flag_timestamp)
                row.append(first_update_timestamp)
                row.append(nombre_declares_active_entrys)
                row.append(nombre_active_entrys)
                row.append(quality_score_metric_A_return_rate)
                row.append(unique_available_flag_count)
                row.append(unique_timestamps)
                row.append(unique_transaction_value_count)
                row.append(unique_return_rate_count)
                row.append(interval_1)
                row.append(interval_2)
                row.append(interval_3)
                row.append(interval_4)
                
                row.append(non_active_entry_count)
                row.append(unique_non_active_entry_count)
                
                row.append(option_type_B_sum_total_transaction_value)
                row.append(option_type_B_sum_difference)
                row.append(option_type_B_sum_difference_ratio)
                row.append(option_type_B_dropped_update_time_count)
                row.append(option_type_B_total_corrections)
                row.append(option_type_B_reducing_update_times_count)
                row.append(option_type_B_unique_update_time_count)
                
                row.append(option_type_A_sum_total_transaction_value)
                row.append(option_type_A_sum_difference)
                row.append(option_type_A_sum_difference_ratio)
                row.append(option_type_A_dropped_update_time_count)
                row.append(option_type_A_total_corrections)
                row.append(option_type_A_reducing_update_times_count)
                row.append(option_type_A_unique_update_time_count)
                
                option_type_B_data = pd.concat([option_type_B_data, option_type_B_data_temp])
                option_type_A_data = pd.concat([option_type_A_data, option_type_A_data_temp])
                
                rows.append(row)
                            
            else:    
                logger.info(f"Fetched data for event_id {event_id} not found, no start_time_reel.")
        else:
            logger.info(f"Fetched data for event_id {event_id} not found, no available_flag_data.")
    except Exception as e:
        logger.error(f"An error occurred: {e} for {event_id} in transaction_value_mod")
        pass            
    return rows, option_type_B_data, option_type_A_data 
#%% main functions

def prepare_session(day_db_format):
    logger = create_logging(config['log_stages']['clean'])
    try:
        with open(config['db_file_directories']['base_target'] + config['db_file_directories']['data_session'] + day_db_format + '.json') as f:
            raw_data = json.load(f)
        raw_data = pd.json_normalize(raw_data, ['programme', 'sessions'])
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        
        day = datetime.fromtimestamp(raw_data['datesession'][0] / 1000)
        day = day.strftime('%Y-%m-%d')
        
        data['session_id'] = np.nan
        data['day'] = day
        data['location_id'] = np.nan
        data['meteo_nebulosite_id'] = np.nan
        
        populate_data_from_raw(data, raw_data, dictionary, 'service_session')
        
        data = resolver(dictionary['foreign_location'], data)
        data = resolver(dictionary['foreign_meteo_nebulosite'], data)
    
        logger.info(f"session data from day {day_db_format} successfully prepared.")
        return data
    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_session")
        return None

#%%
def prepare_events(day_db_format):
    try:
        logger = create_logging(config['log_stages']['clean'])
        try:
            with open(config['db_file_directories']['base_target'] + config['db_file_directories']['data_session'] + day_db_format + '.json', 'Z') as f:
                raw_data = json.load(f)
        except Exception as e:
            logger.error(f"An error occurred: {e} in prepare_events file selecting")
            pass
            
        raw_data = pd.json_normalize(raw_data, ['programme', 'sessions', ['events']])
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        timezone_offset = raw_data['timezoneOffset'][0]
        raw_data['start_time'] = raw_data['start_time'] + (timezone_offset)
        day = datetime.fromtimestamp(raw_data['start_time'][0] / 1000)
        day = day.strftime('%Y-%m-%d')    
        
        reference_id = (day_db_format + '_R' + raw_data['numsession'].astype(str) + 'F' + raw_data['numOrder'].astype(str)).astype(str)
        
        data['event_id'] = np.nan
        data['reference_id'] = reference_id
        data['day'] = day
        data['session_id'] = np.nan
        data['category_id'] = np.nan
        
        if 'condition_metric.valeurMesure' in raw_data.columns:
            raw_data['condition_metric.valeurMesure'] = raw_data['condition_metric.valeurMesure'].apply(lambda x: str(x).replace(',', '.') if pd.notna(x) else x)
        
        populate_data_from_raw(data, raw_data, dictionary, 'service_event')
        
        data = resolver(dictionary['foreign_session'], data)
        data = resolver(dictionary['foreign_category'], data)
        
        data['event_start_time'] = pd.to_datetime(data['event_start_time'] / 1000, unit='s')
        data['start'] = data['status'].apply(lambda x: True if x == 'finish' else False)
        data['start'] = data['start'].astype('boolean')
        
        logger.info(f"event data from day {day_db_format} successfully prepared.")
    
        return data, timezone_offset
    
    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_events file selecting")
        return None

#%%
def prepare_entitys(day_db_format, timezone_offset):
    logger = create_logging(config['log_stages']['clean'])
    try:
        raw_data = pd.DataFrame([])
        try:
            os.chdir(config['db_file_directories']['base_target'] + config['db_file_directories']['data_entitys'] + day_db_format)
            for root, dirs, files in os.walk("."):
                for file in files:
                    df_temp = pd.read_csv(file)
                    name = file.split('.csv')[0]
                    df_temp['reference_id'] =  day_db_format + '_' + name
                    num_session = file.split('F')[0].split('.csv')[0]
                    num_session = int(num_session.split('Z')[1])
                    df_temp['num_session'] = num_session
                    num_event = int(file.split('F')[1].split('.csv')[0])
                    df_temp['num_event'] = num_event
                    modification_time = os.path.getmtime(file)
                    local_time = time.ctime(modification_time)
                    df_temp['collected_at'] = local_time
                    raw_data = pd.concat([raw_data, df_temp])
        except Exception as e:
            logger.error(f"An error occurred: {e} in prepare_entitys file selecting")
            pass
    
        os.chdir(r'C:\Projekt\service\2_program\service_program')
        
        raw_data.drop(columns=['Unnamed: 0'], axis=1, inplace=True)
        raw_data = raw_data.reset_index(drop=True)
        
        day = pd.to_datetime(day_db_format, format='%Y%m%d').date()
        
        # Instantiate the class for each column
        operator_cleaner = NameCleaner('operator')
        supervisor_cleaner = NameCleaner('supervisor')
        owner_cleaner = NameCleaner('owner')
        
        # Clean each column
        raw_data = operator_cleaner.clean_column(raw_data, 0)
        raw_data = supervisor_cleaner.clean_column(raw_data, 2)
        raw_data = owner_cleaner.clean_column(raw_data, 3)
        try:
            organization_cleaner = NameCleaner('organization')
            raw_data = organization_cleaner.clean_column(raw_data, 2)
            organization = True
        except Exception as e:
            logger.error(f"An error for NameCleaner('organization') occurred: {e}")
            organization = False
            pass
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        
        data['entity_id'] = np.nan
        data['event_id'] = np.nan
        data['session_id'] = np.nan
        data['day'] = day
        data['subject_id'] = np.nan
        data['operator_id'] = np.nan
        data['distance_subject_id'] = np.nan
        
        populate_data_from_raw(data, raw_data, dictionary, 'service_entity')
        
        data['non_active_entry'] = data['non_active_entry'].map({'NON_active_entry': True, 'active_entry': False})
        data['non_active_entry'] = data['non_active_entry'].astype('boolean')
    
        data['metric_A_timestamp'] = pd.to_datetime(data['metric_A_timestamp'] / 1000 + timezone_offset / 1000, unit='s')
        data['metric_B_timestamp'] = pd.to_datetime(data['metric_B_timestamp'] / 1000 + timezone_offset / 1000, unit='s')
        
        data['result_order'] = data['result_order'].fillna(0)
        
        data = data.reset_index(drop=True)
        
        data = resolver(dictionary['foreign_distance_subject'], data)
        data = resolver(dictionary['foreign_session'], data)
        data = resolver(dictionary['foreign_event'], data)
        data = resolver(dictionary['foreign_subject'], data)
        data = resolver(dictionary['foreign_operator'], data)
        
        # Assuming the existing DataFrame is `data` and it contains 'supervisor1', 'supervisor2', 'supervisor3', 'event_id', and 'num_service'
        # Step 1: Select the relevant columns
        supervisor_columns = ['supervisor1', 'supervisor2', 'supervisor3', 'supervisor4']
        meta_columns = ['event_id', 'num_service']
        
        # Step 2: Melt the DataFrame to combine supervisors into one column
        supervisor_data = data[meta_columns + supervisor_columns].melt(id_vars=meta_columns, 
                                                               value_vars=supervisor_columns, 
                                                               var_name='supervisor_type', 
                                                               value_name='supervisor_name')
        
        # Step 3: Drop rows where 'supervisor' is NaN
        supervisor_data = supervisor_data.dropna(subset=['supervisor_name'])
        
        # Step 4: Optionally reset the index for clean ordering
        supervisor_data = supervisor_data.reset_index(drop=True)
    
        supervisor_data = resolver(dictionary['foreign_supervisor'], supervisor_data)
    
        owner_columns = ['owner1', 'owner2', 'owner3', 'owner4']
        meta_columns = ['event_id', 'num_service']
        owner_data = data[meta_columns + owner_columns].melt(id_vars=meta_columns, 
                                                               value_vars=owner_columns, 
                                                               var_name='owner_type', 
                                                               value_name='owner_name')
        owner_data = owner_data.dropna(subset=['owner_name'])
        owner_data = owner_data.reset_index(drop=True)
    
        owner_data = resolver(dictionary['foreign_owner'], owner_data)
        
        if organization == False:
            organization_data = None
        else:
            organization_columns = ['organization1', 'organization2', 'organization3', 'organization4']
            meta_columns = ['event_id', 'num_service']
            organization_data = data[meta_columns + organization_columns].melt(id_vars=meta_columns, 
                                                                   value_vars=organization_columns, 
                                                                   var_name='organization_type', 
                                                                   value_name='organization_name')
            organization_data = organization_data.dropna(subset=['organization_name'])
            organization_data = organization_data.reset_index(drop=True)
            
            organization_data = resolver(dictionary['foreign_organization'], organization_data)
        
        logger.info(f"entity data from day {day_db_format} successfully prepared.")
    
        return data, supervisor_data, owner_data, organization_data
    
    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_entitys")
        return None, None, None, None

#%% 
def prepare_return_rate_definitive_offline(day_db_format):
    logger = create_logging(config['log_stages']['clean'])
    try:
        raw_data = pd.DataFrame([])
        try:
            # Set the working directory
            os.chdir(config['db_file_directories']['base_target'] + config['db_file_directories']['data_return_rate_definitive'] + day_db_format)
            # Traverse all files in the directory
            for root, dirs, files in os.walk("."):
                for file in files:
                    file_path = os.path.join(root, file)
                    with open(file_path, 'Z') as f:
                        json_data = json.load(f)
                        # Convert to DataFrame (assuming JSON data is structured for it)
                        df_temp = pd.json_normalize(json_data, "return_rates", ["typeoperation", "input_valueBase", "adjusted_value", "observation_type", "familleoperation"])  # Adjust as needed for your JSON structure
                    num_session = file.split('F')[0].split('.json')[0]
                    num_session = int(num_session.split('Z')[1])
                    df_temp['num_session'] = num_session
                    num_event = int(file.split('F')[1].split('.json')[0])
                    df_temp['num_event'] = num_event
                    modification_time = os.path.getmtime(file)
                    for index, row in df_temp.iterrows():
                        increment_ms = 0.001 * (index)
                        modified_time  = modification_time + increment_ms
                        modified_datetime  = datetime.fromtimestamp(modified_time)
                        formatted_time = modified_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Truncate to milliseconds
                        df_temp.at[index, 'collected_at'] = formatted_time
                    raw_data = pd.concat([raw_data, df_temp])
        except Exception as e:
            logger.error(f"An error occurred: {e} in prepare_return_rate_definitive_offline file selecting")
            pass    
        
        os.chdir(r'C:\Projekt\service\2_program\service_program')
        
        raw_data = raw_data.reset_index(drop=True)
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        
        day = pd.to_datetime(day_db_format, format='%Y%m%d').date()
        
        data['event_id'] = np.nan
        data['session_id'] = np.nan
        data['day'] = day
        data['operation_type_id'] = np.nan
        data['operation_type_libelle_id'] = np.nan
    
        populate_data_from_raw(data, raw_data, dictionary, 'service_return_rate_definitive_offline')    
    
        data = resolver(dictionary['foreign_operation_type_libelle'], data)
        data = resolver(dictionary['foreign_operation_type'], data)
        data = resolver(dictionary['foreign_session'], data)
        data = resolver(dictionary['foreign_event'], data)
    
        logger.info(f"return_rate_definitive_offline data from day {day_db_format} successfully prepared.")
        
        return data

    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_return_rate_definitive_offline")
        return None 

#%%
def prepare_combinaison_data(day_db_format, raw_data, entity_data):
    logger = create_logging(config['log_stages']['clean'])
    
    # def preprocess_combinaison(comb_set):
    #     return {99 if val == 'NP' or val == 'Autres' else int(val) for val in comb_set}
    def preprocess_combinaison(comb_set):
        processed_set = set()
        for val in comb_set:
            if val == 'NP':
                processed_set.add(99)
            else:
                try:
                    # Try converting val to an integer; if successful, add it
                    processed_set.add(int(val))
                except ValueError:
                    # If conversion fails, it's not a number; add 98
                    processed_set.add(98)
        return processed_set

    try:
        raw_data['combinaison'] = raw_data['combinaison'].apply(preprocess_combinaison)
    
        expanded_rows = []
        for idx, row in raw_data.iterrows():
            for pos, value in enumerate(sorted(row['combinaison'])):  # Sorting ensures consistency in position
                expanded_rows.append({
                    'return_rate_definitive_id': row['return_rate_definitive_id'],
                    'operation_type_id': row['operation_type_id'],
                    'event_id': row['event_id'],
                    'combinaison_position': pos + 1, # Track the position
                    'num_service': value
                })
        
        expanded_df = pd.DataFrame(expanded_rows)
        filtered_df = expanded_df.loc[(expanded_df['num_service'] != 99) & (expanded_df['num_service'] != 98)].copy()
        filtered_df_99_98 = expanded_df.loc[(expanded_df['num_service'] == 99) | (expanded_df['num_service'] == 98)].copy()
        filtered_df_99_98['entity_id'] = np.nan
        
        filtered_df['entity_id'] = filtered_df[['num_service', 'event_id']].apply(
            lambda row: entity_data.loc[
                (entity_data['num_service'] == row['num_service']) & (entity_data['event_id'] == row['event_id']),
                'entity_id'
            ].values[0] if not entity_data.loc[
                (entity_data['num_service'] == row['num_service']) & (entity_data['event_id'] == row['event_id']),
                'entity_id'
            ].empty else None,
            axis=1
        )
        
        # filtered_df = resolver(dictionary['foreign_entity'], filtered_df)

        data = pd.concat([filtered_df, filtered_df_99_98])
        data = data.sort_index()     
    
        logger.info(f"combinaison_data data from day {day_db_format} successfully prepared.")
            
        return data

    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_combinaison_data")
        return None 

#%%
def prepare_transaction_value(day_db_format):
    logger = create_logging(config['log_stages']['clean'])
    try:
        file = config['db_file_directories']['base_target'] + config['db_file_directories']['transaction_value'] + day_db_format + '.csv'
        raw_data = pd.read_csv(file)
        raw_data = raw_data.rename(columns={'totaltransaction_value': 'sum_totaltransaction_value'})
        raw_data[['combinaison', 'totaltransaction_value']] = raw_data['listeCombinaisons'].apply(lambda x: pd.Series(ast.literal_eval(x)))
        raw_data['combinaison'] = raw_data['combinaison'].apply(format_values)
        timezone_offset = raw_data['updateTimeOffset'][0] / 1000
        raw_data = raw_data.drop(columns=['listeCombinaisons', 'Unnamed: 0', 'index', 'updateTimeOffset', 'dateProgramme'])
        raw_data['updateTime'] = pd.to_datetime(raw_data['updateTime'] / 1000 + timezone_offset, unit='s')
        raw_data['collected_at'] = pd.to_datetime(raw_data['collected_at'] + timezone_offset, unit='s')
    
        day = pd.to_datetime(day_db_format, format='%Y%m%d').date()
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        
        data['day'] = day
        data['event_id'] = np.nan
        data['operation_type_libelle_id'] = np.nan
        
        populate_data_from_raw(data, raw_data, dictionary, 'service_transaction_value')
        
        data = resolver(dictionary['foreign_event'], data)
        data = resolver(dictionary['foreign_operation_type_libelle_transaction_value'], data)
        
        logger.info(f"transaction_value_raw data from day {day_db_format} successfully prepared.")
    
        return data, timezone_offset

    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_transaction_value")
        return None

def prepare_return_rate(day_db_format, timezone_offset):
    logger = create_logging(config['log_stages']['clean'])
    try:
        file = config['db_file_directories']['base_target'] + config['db_file_directories']['return_rate'] + day_db_format + '.csv'
        raw_data = pd.read_csv(file)
        raw_data['updateTime'] = pd.to_datetime(raw_data['live_update_ts'] / 1000 + timezone_offset, unit='s')
        raw_data['collected_at'] = pd.to_datetime(raw_data['collected_at'] + timezone_offset, unit='s')
        raw_data = raw_data.drop(columns=['live_update_ts', 'numerosentity', 'is_selected', 'return_rateReference', 'tendance', 'large_outcome', 'significant_change', 'typeoperation'])

        day = pd.to_datetime(day_db_format, format='%Y%m%d').date()
        
        data = pd.DataFrame(index=range(len(raw_data)),)
        
        data['day'] = day
        data['event_id'] = np.nan
        
        populate_data_from_raw(data, raw_data, dictionary, 'service_return_rate')
        
        data = resolver(dictionary['foreign_event'], data)
        # data = resolver(dictionary['foreign_entity'], data)
        
        logger.info(f"return_rate_raw data from day {day_db_format} successfully prepared.")
    
        return data 

    except Exception as e:
        logger.error(f"An error occurred: {e} in prepare_return_rate")
        return None

#%%