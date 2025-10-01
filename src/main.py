#%% notes



#%% modules
import os
os.chdir(r'C:\Projekt\service\2_program\service_program')


import pandas as pd
from datetime import datetime, timedelta
import time

from utils import load_yaml, create_logging, retry, generate_history, create_subdirectories, combine_daily_csv_files_by_day, copy_csv_files
from data_manager import DataManager
from threads import MyThread
from data_collection import fetch_base_data, make_event_list, fetch_entitys_data, fetch_return_rate_definitives_data
from data_cleaning import transaction_value_mod, prepare_session, prepare_events, prepare_entitys, prepare_return_rate_definitive_offline, prepare_combinaison_data, prepare_transaction_value, prepare_return_rate
from data_integration import insert_data, resolver, bulk_data, fetch_data_from_db, DatabaseConnection

config = load_yaml('config')
dictionary = load_yaml('dictionary')
keys = load_yaml('basic_primary_keys_list')

#%% 
"""
.
"""

def service_main():
    
    """
    Here we will put the opening actions when the program is starting in the morning:
        1.  It fetchs all base raw entity and event data ( day -1 ) so all base data from yesterday.
            After that it integrate them after cleaning into the database ( cleaner.py and etl.py ).
        2.  Now we combine and clean all of the fetched raw_transaction_value data from yesterday ( cleaner.py ) and integrate them also ( etl.py )
        3.  a) Then we communicate ( bot.py ) the results and important information ( etl.py ) of yesterday ( day - 1 ). Wich are already 
               elaborated yesterday evening by analyzer.py and integrated by etl.py into my database(table results)
            b) step 3 creates also a dictionnary that can be callable through the bot.py
        4.  Now we start our day while fetching and cleaning the base data of events and creating the event_list ( fetcher.py, cleaner.py and etl.py ).
            And communicate them through discord ( bot.py ).
    """
    
    """
    1.First check how much time we have to procced data collecting, cleaning and integration on previous days before the first event starts.
    2.fetch history base data.
    """

    logger = create_logging(config['log_stages']['main'])
    logger.info("starting service_program...")
    
    today = datetime.today()
    day_service_format = datetime.strftime(datetime.today(), "%d%m%Y")
    day_db_format = datetime.strftime(datetime.today(), "%Y%m%d")
    base_data = retry(lambda: fetch_base_data(0, day_service_format, day_db_format), "main", retries = config['fetch_config']['fetch_primary_data_retries'], delay = config['fetch_config']['fetch_primary_data_delay'])
    event_list = make_event_list(base_data, day_db_format)
    
    first_event = event_list[event_list['Online'] == False].loc[event_list[event_list['Online'] == False]['start_time'].idxmin()] if not event_list[event_list['Online'] == False].empty else None
    first_event_start = datetime.fromtimestamp(first_event['start_time'])
    
    now = datetime.now()
    time_diff = first_event_start - now
    resting_time = (time_diff.total_seconds() / 60 ) - config['fetch_config']['resting_time']
    
    if resting_time > 0:
        db = DatabaseConnection(config)
        db.connect()
        try:
            with db.get_cursor() as cursor:
                for i in range(10):  # Corrected loop
                    try:
                        # Call the function to update entitys history
                        cursor.execute("SELECT update_entitys_history('subject', 1);")
                        cursor.execute("SELECT update_entitys_history('operator', 1);")
                        cursor.execute("SELECT update_entitys_history('owner', 1);")
                        cursor.execute("SELECT update_entitys_history('supervisor', 1);")
                        logger.info(f"Iteration {i+1}: Updated entitys history successfully!")
                        db.db_commit()

                    except Exception as e: 
                        logger.error(f"Error updating entitys history on iteration {i+1}: {e}")
                        pass
                    
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            pass
            
        finally:
            db.close()
            logger.info("Inserted entity and updated history successfully!")
    # logger.info(f"time until threads has to start: {resting_time}")

    # days_to_check = int(resting_time // config['fetch_config']['resting_time_diviser'])

    # query = "SELECT DISTINCT day FROM session"
    # db_existing_data = fetch_data_from_db(query, config)
    # db_existing_data = db_existing_data['day'].apply(lambda x: x.strftime("%Y%m%d")).tolist()

    # history = generate_history(db_existing_data, days_to_check)
    
    # if len(history) > 0:
        
    no_valid_date = False    
    while resting_time > 0 and no_valid_date == False:
        
        now = datetime.now()
        time_diff = first_event_start - now
        resting_time = ( time_diff.total_seconds() / 60 ) - config['fetch_config']['resting_time']
        logger.info(f"time until threads has to start: {resting_time}")
        
        query = "SELECT DISTINCT day FROM session"
        db_existing_data = fetch_data_from_db(query, config)
        db_existing_data = db_existing_data['day'].apply(lambda x: x.strftime("%Y%m%d")).tolist()


        if int(min(db_existing_data)) < int(config['fetch_config']['end_date']):
            logger.info("end date reached!!!")
            no_valid_date = True
        
        history, no_valid_date = generate_history(db_existing_data, 1)
        
        if no_valid_date:
            logger.info("end date reached!!!")

        for day in history:
            try:
                db = DatabaseConnection(config)
                db.connect()
                with db.get_cursor() as cursor:
                    try:
                        # Call the function to update entitys history
                        cursor.execute("SELECT update_entitys_history('subject', 1);")
                        cursor.execute("SELECT update_entitys_history('operator', 1);")
                        cursor.execute("SELECT update_entitys_history('owner', 1);")
                        cursor.execute("SELECT update_entitys_history('supervisor', 1);")

                        logger.info("Updated entitys history successfully!")
                        db.db_commit()

                    except Exception as e:
                        logger.error(f"Error updating entitys history: {e}")
                        pass
                        
            except Exception as e:
                logger.error(f"Database connection error: {e}")
                pass
            finally:
                db.close()
                
            try:
            
                logger.info(f"integrate data into database for day: {day}")
                day_dt = datetime.strptime(day, "%Y%m%d")
                timedelta_days = (today - day_dt).days  # .days gives the difference in days
                day_service_format = datetime.strftime(datetime.today() - timedelta(days=timedelta_days), "%d%m%Y")
                day_db_format = datetime.strftime(datetime.today() - timedelta(days=timedelta_days), "%Y%m%d")
                base_data = retry(lambda: fetch_base_data(timedelta_days, day_service_format, day_db_format), "main", retries = config['fetch_config']['fetch_primary_data_retries'], delay = config['fetch_config']['fetch_primary_data_delay'])
                event_list = make_event_list(base_data, day_db_format)
                
                folder_names = [name for name in os.listdir(config['db_file_directories']['base_target'] + config['db_file_directories']['data_entitys']) if os.path.isdir(os.path.join(config['db_file_directories']['base_target'] + config['db_file_directories']['data_entitys'], name))]
                if not day_db_format in folder_names:
                    fetch_entitys_data(event_list, day_service_format, day_db_format)
                folder_names = [name for name in os.listdir(config['db_file_directories']['base_target'] + config['db_file_directories']['data_return_rate_definitive']) if os.path.isdir(os.path.join(config['db_file_directories']['base_target'] + config['db_file_directories']['data_return_rate_definitive'], name))]
                if not day_db_format in folder_names:
                    fetch_return_rate_definitives_data(event_list, day_service_format, day_db_format)
                    
    
                data = prepare_session(day_db_format)
                if data is not None:
                    insert_data(data, 'session', (dictionary['session']['columns']), day_db_format)
    
                    data, timezone_offset = prepare_events(day_db_format)
                    if data is not None:
                        insert_data(data, 'event', (dictionary['event']['columns']), day_db_format)

                    data, supervisor_data, owner_data, organization_data = prepare_entitys(day_db_format, timezone_offset)
                    
                    if data is not None:
                        data = insert_data(data, 'entity', (dictionary['entity']['columns']), day_db_format, 'entity_id')
                        entity_data = data[['entity_id', 'event_id', 'num_service']]
                        
                        if supervisor_data is not None:
                            supervisor_data = resolver(dictionary['foreign_entity'], supervisor_data)
                            insert_data(supervisor_data, 'entity_supervisor', (dictionary['entity_supervisor']['columns']), day_db_format)
                        
                        if owner_data is not None:
                            owner_data = resolver(dictionary['foreign_entity'], owner_data)
                            insert_data(owner_data, 'entity_owner', (dictionary['entity_owner']['columns']), day_db_format)
                            
                        if organization_data is not None:
                            organization_data = resolver(dictionary['foreign_entity'], organization_data)
                            insert_data(organization_data, 'entity_organization', (dictionary['entity_organization']['columns']), day_db_format)
                    
                    del supervisor_data, owner_data, organization_data
                    
                    data = prepare_return_rate_definitive_offline(day_db_format)
                    if data is not None:
                        combinaison_raw_data = insert_data(data, 'return_rate_definitive_offline', (dictionary['return_rate_definitive_offline']['columns']), day_db_format, 'return_rate_definitive_id')
                    
                        data = prepare_combinaison_data(day_db_format, combinaison_raw_data, entity_data)
                        if data is not None:
                            insert_data(data, 'combinaison', (dictionary['combinaison']['columns']), day_db_format)
                    
                    # day_db_format = '20240630'
                    folder_names = [os.path.splitext(name)[0] for name in os.listdir(config['db_file_directories']['base_target'] + config['db_file_directories']['transaction_value']) if os.path.isfile(os.path.join(config['db_file_directories']['base_target'] + config['db_file_directories']['transaction_value'], name)) and name.endswith('.csv')]
                    if day_db_format in folder_names:
                        data, timezone_offset = prepare_transaction_value(day_db_format)
                        transaction_value_success_flag_data = data.loc[(data['operation_type_libelle_id'] == keys['operation_type_libelle_id']['option_type_B']) | (data['operation_type_libelle_id'] == keys['operation_type_libelle_id']['option_type_B_INTERNATIONAL'])].copy()
                        transaction_value_place_data = data.loc[(data['operation_type_libelle_id'] == keys['operation_type_libelle_id']['option_type_A']) | (data['operation_type_libelle_id'] == keys['operation_type_libelle_id']['option_type_A_INTERNATIONAL'])].copy()
                        bulk_data(data, 'transaction_value_raw', (dictionary['transaction_value']['columns']))
                    else:
                        transaction_value_success_flag_data = pd.DataFrame([])
                        transaction_value_place_data = pd.DataFrame([])
                        logger.info(f"transaction_value raw data from day {day_db_format} not founded in raw data.")
                        
                    folder_names = [os.path.splitext(name)[0] for name in os.listdir(config['db_file_directories']['base_target'] + config['db_file_directories']['return_rate']) if os.path.isfile(os.path.join(config['db_file_directories']['base_target'] + config['db_file_directories']['return_rate'], name)) and name.endswith('.csv')]
                    if day_db_format in folder_names:
                        return_rate_data = prepare_return_rate(day_db_format, timezone_offset)
                        bulk_data(return_rate_data, 'return_rate_raw', (dictionary['return_rate']['columns']))
                    else:
                        return_rate_data = pd.DataFrame([])
                        logger.info(f"return_rate raw data from day {day_db_format} not founded in raw data.")
                    
                    if not transaction_value_success_flag_data.empty and not transaction_value_place_data.empty and not return_rate_data.empty:
                        event_info_data, option_type_B_data, option_type_A_data = transaction_value_mod(day_db_format, transaction_value_success_flag_data, return_rate_data, transaction_value_place_data)
                        if not event_info_data.empty and not option_type_B_data.empty and not option_type_A_data.empty:
                            insert_data(event_info_data, 'event_info_transaction_value_data', (dictionary['event_info_transaction_value_data']['columns']), day_db_format)
                            bulk_data(option_type_B_data, 'transaction_value_option_type_B_mod', (dictionary['transaction_value_option_type_B_mod']['columns']))
                            bulk_data(option_type_A_data, 'transaction_value_option_type_A_mod', (dictionary['transaction_value_option_type_A_mod']['columns']))
                    else:
                        event_info_data = pd.DataFrame([])
                        option_type_B_data = pd.DataFrame([])
                        option_type_A_data = pd.DataFrame([])
                        logger.info(f"no transaction_value mod data from day {day_db_format} in Database integrated.")
        
                    del combinaison_raw_data, data, return_rate_data, event_info_data, transaction_value_success_flag_data, transaction_value_place_data, option_type_B_data, option_type_A_data
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                pass

    """
    fetch the base data for today and create a event list used by the threads to steer their events onward.
    """
    day_service_format = datetime.strftime(datetime.today(), "%d%m%Y")
    day_db_format = datetime.strftime(datetime.today(), "%Y%m%d")
    base_data = retry(lambda: fetch_base_data(0, day_service_format, day_db_format), "main", retries = config['fetch_config']['fetch_primary_data_retries'], delay = config['fetch_config']['fetch_primary_data_delay'])
    event_list = make_event_list(base_data, day_db_format)
    # fetch_entitys_data(event_list, day_service_format, day_db_format)


    base_path = config['program_file_directories']['base_target']
    timing_list = pd.read_csv(base_path + 'service_program' + "/" + 'timing_list.csv')
    
    logger.info("starting threads !")
    threads = [MyThread(i, event_list, timing_list) for i in range(config['fetch_config']['event_list_length'] * 2)] # opens up to 120 threads each of them works for one event

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    """
    Here we will put the closing actions when the program has ended the last thread:
        1.  Combine and analyzing ( analyzer.py ) the results of the day and integrate them into the database. 
        2.  Creating a dictionnary of this results and communicate them ( bot.py )
        3.  Then through discord stop the service_main.py and take the pc to sleep ( bot.py )
    """

    combine_daily_csv_files_by_day(f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['program_file_directories']['transaction_value_data']}", 
                                    f"{config['db_file_directories']['base_target']}{config['db_file_directories']['transaction_value']}",
                                    day_db_format)
    combine_daily_csv_files_by_day(f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['program_file_directories']['return_rate_data']}", 
                                    f"{config['db_file_directories']['base_target']}{config['db_file_directories']['return_rate']}",
                                    day_db_format)
    
    copy_csv_files(f"{config['program_file_directories']['base_target']}{config['program_file_directories']['daily_transaction_value_data']}{config['program_file_directories']['available_flag_data']}" + day_db_format, 
                    f"{config['db_file_directories']['base_target']}{config['db_file_directories']['available_flag']}" + day_db_format)
    
    logger.info("done")
    
if __name__ == "__main__":
    service_main()
    print("done !!!")