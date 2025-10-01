#%% notes



#%% modules
import os
os.chdir(r'C:\Projekt\service\2_program\service_program')

import pandas as pd
import json
import random
import time
import requests

from my_functions import load_yaml, create_logging, create_subdirectories, retry
from data_manager import DataManager

config = load_yaml('config')

#%% functions

def generate_dummy_entries(num_entries, event_list):
    dummy_entries = []
    for _ in range(num_entries):
        dummy_entry = {
            'start_time': event_list['start_time'].iloc[0],
            'Z': 99,
            'F': 99,
            'Online': 'True',
            'observation_type': 'dummy',
            'typeoperation': 'dummy',
        }
        dummy_entries.append(dummy_entry)
    return dummy_entries

def fetch_entity_data(event_list, day_service_format, day_db_format, i, daily_data = False):
    logger = create_logging(config['log_stages']['fetch'])
    
    try:
        # Fetch data
        url = f"{config['data_sources']['base-url']}{day_service_format}/Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}/entitys"
        content_entitys_raw = requests.get(url)
        content_entitys_raw.raise_for_status()  # Raise error for HTTP issues

        # Process and save data
        content_entitys = content_entitys_raw.json()
        df_content_entitys = pd.json_normalize(content_entitys, ['entitys'])
        if daily_data == True:
            output_path = f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['db_file_directories']['data_entitys']}{day_db_format}/Z{event_list.loc[i, 'Z']}F{event_list.loc[i, 'F']}.csv"
            os.makedirs(f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['db_file_directories']['data_entitys']}{day_db_format}", exist_ok=True)
        else:
            output_path = f"{config['db_file_directories']['base_target']}{config['db_file_directories']['data_entitys']}{day_db_format}/Z{event_list.loc[i, 'Z']}F{event_list.loc[i, 'F']}.csv"
        df_content_entitys.to_csv(output_path)

        logger.info(f"entitys data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']} successfully collected.")
    except Exception as e:
        logger.error(f"Failed to collect entity data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}: {e}")
        raise  # Re-raise to handle in retry
        
def fetch_return_rate_definitive_data(event_list, day_service_format, day_db_format, i, daily_data = False):
    logger = create_logging(config['log_stages']['fetch'])
    try:
        # Fetch data
        url = f"{config['data_sources']['base-url']}{day_service_format}/Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}{config['data_sources']['return_rate_definitive']}"
        content_return_rate_definitive_raw = requests.get(url)
        content_return_rate_definitive_raw.raise_for_status()  # Raise error for HTTP issues

        # Process and save data
        content_return_rate_definitive = content_return_rate_definitive_raw.json()
        if daily_data == True:
            output_path = f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['db_file_directories']['data_return_rate_definitive']}{day_db_format}/Z{event_list.loc[i, 'Z']}F{event_list.loc[i, 'F']}"
            os.makedirs(f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['db_file_directories']['data_return_rate_definitive']}{day_db_format}", exist_ok=True)
        else:
            output_path = f"{config['db_file_directories']['base_target']}{config['db_file_directories']['data_return_rate_definitive']}{day_db_format}/Z{event_list.loc[i, 'Z']}F{event_list.loc[i, 'F']}"
        with open(output_path + '.json', 'w') as p:
            json.dump(content_return_rate_definitive, p) 

        logger.info(f"return_rate_definitive data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']} successfully collected.")
    except Exception as e:
        logger.error(f"Failed to collect return_rate_definitive data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}: {e}")
        raise  # Re-raise to handle in retry        

#%% main functions
# ###################################
def fetch_base_data(timedelta_days, day_service_format, day_db_format):
    logger = create_logging(config['log_stages']['fetch'])
    try:
        folder_names = [os.path.splitext(name)[0] for name in os.listdir(config['db_file_directories']['base_target'] + config['db_file_directories']['data_session']) if os.path.isfile(os.path.join(config['db_file_directories']['base_target'] + config['db_file_directories']['data_session'], name)) and name.endswith('.json')]
        if not day_db_format in folder_names:
            # If the data for the current day doesnt exists, fetch and process it
            base_url = config['data_sources']['base-url'] + day_service_format
            content_day_raw = requests.get(base_url)
            content_day = content_day_raw.json()
            if timedelta_days != 0:
                with open(config['db_file_directories']['base_target'] + config['db_file_directories']['data_session'] + day_db_format + '.json', 'w') as p:
                    json.dump(content_day, p) 
                logger.info(f"Base data for {day_db_format} succesfully saved.")
            logger.info(f"Base data for {day_db_format} succesfully collected.")  
        
            data = pd.json_normalize(content_day, ['programme', 'sessions', ['events']])
            data = pd.concat([data.drop(['operations'], axis=1), data['operations'].apply(pd.Series)], axis=1)
            data = pd.concat([data.drop([0], axis=1), data[0].apply(pd.Series)], axis=1)
        else:
            # If the data for the current day already exists, load and process it
            logger.info(f"Data for {day_db_format} already exists. Proceeding with data processing.")
    
            with open(config['db_file_directories']['base_target'] + 
                      config['db_file_directories']['data_session'] + 
                      day_db_format + '.json', 'Z') as p:
                content_day = json.load(p)
            data = pd.json_normalize(content_day, ['programme', 'sessions', ['events']])
            data = pd.concat([data.drop(['operations'], axis=1), data['operations'].apply(pd.Series)], axis=1)
            data = pd.concat([data.drop([0], axis=1), data[0].apply(pd.Series)], axis=1)
    
            logger.info(f"Base data for {day_db_format} successfully loaded and normalized.")
            
        return data
    
    except Exception as e:
        logger.critical(f"Error loading or processing the base data for {day_db_format}: {e}")
        raise

def make_event_list(df_events, day_db_format):
    logger = create_logging(config['log_stages']['fetch'])
    try:
        event_list_path = config['program_file_directories']['base_target'] + config['program_file_directories']['event_list'] + day_db_format + '.csv'
        event_list = pd.DataFrame().assign(start_time=df_events['start_time']//1000, R=df_events['numsession'], C=df_events['numOrdre'], Online=df_events['courseExclusiveInternet'], observation_type=df_events['observation_type'], typeoperation=df_events['typeoperation'])
    
        original_dtypes = event_list.dtypes
    
        num_existing_entries = len(event_list)
        desired_num_entries = config['fetch_config']['event_list_length']
    
        entries_to_add = desired_num_entries - num_existing_entries
    
        dummy_entries = generate_dummy_entries(entries_to_add, event_list)
        dummy_data = pd.DataFrame(dummy_entries)
    
        event_list = pd.concat([event_list, dummy_data], ignore_index=False)
        event_list.reset_index(drop=True, inplace=True)
    
        event_list = event_list.astype(original_dtypes)
    
        DataManager.save_data(event_list, event_list_path)
        return event_list
    
    except Exception as e:
        logger.error(f"An error occurred: {e} in make_event_list")
        return None

def fetch_entitys_data(event_list, day_service_format, day_db_format):
    logger = create_logging(config['log_stages']['fetch'])
    create_subdirectories(config['db_file_directories']['base_target'] + config['db_file_directories']['data_entitys'], day_db_format)
    
    for i, row in event_list.iterrows():
        if row['observation_type'] != 'dummy':
            random_sleep_interval = random.uniform(1000, 3000)
            time.sleep(random_sleep_interval / config['fetch_config']['sleep_time'])
            try:
                retry(lambda: fetch_entity_data(event_list, day_service_format, day_db_format, i), retries = config['fetch_config']['fetch_second_data_retries'], delay = config['fetch_config']['fetch_second_data_delay'])
            except Exception as e:
                logger.critical(f"Failed to collect entity data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}: {e}")
        
def fetch_return_rate_definitives_data(event_list, day_service_format, day_db_format):
    logger = create_logging(config['log_stages']['fetch'])
    create_subdirectories(config['db_file_directories']['base_target'] + config['db_file_directories']['data_return_rate_definitive'], day_db_format)
    
    for i, row in event_list.iterrows():
        if ((row['observation_type'] == 'NATIONAL' and row['Online'] == False) or (row['observation_type'] == 'INTERNATIONAL' and row['Online'] == False)):
            random_sleep_interval = random.uniform(1000, 3000)
            time.sleep(random_sleep_interval / config['fetch_config']['sleep_time'])
            try:
                if day_db_format < config['fetch_config']['low_date']:
                    retry(lambda: fetch_return_rate_definitive_data(event_list, day_service_format, day_db_format, i), retries = config['fetch_config']['fetch_second_data_low_date_retries'], delay = config['fetch_config']['fetch_second_data_low_date_delay'])
                else:
                    retry(lambda: fetch_return_rate_definitive_data(event_list, day_service_format, day_db_format, i), retries = config['fetch_config']['fetch_second_data_retries'], delay = config['fetch_config']['fetch_second_data_delay'])
            except Exception as e:
                logger.critical(f"Failed to collect return_rate_definitive data from day {day_db_format} for Z{event_list.loc[i, 'Z']}/F{event_list.loc[i, 'F']}: {e}")
                
