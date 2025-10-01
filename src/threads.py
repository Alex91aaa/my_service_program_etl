#%% notes


#%% Proxys
proxies = {
''' #############'''
}
proxies_2 = {
''' #############'''
}

#%% modules

import os
os.chdir(r'C:\Projekt\service\2_program\service_program')

import requests
from datetime import datetime
import pandas as pd
import time
import random
import sched
import threading

from my_functions import load_yaml, create_logging, retry
from data_collection import fetch_entity_data

now = time.time()

config = load_yaml('config')
dictionary = load_yaml('dictionary')
keys = load_yaml('basic_primary_keys_list')
#%%

logger = create_logging(config['log_stages']['threads'])

today = datetime.today()
base = config['data_sources']['base-url'] # base-url
day = datetime.strftime(today, "%d%m%Y") #'JJ/MM/AAAA/' # Tages Programm
day_db_format = datetime.strftime(today, "%Y%m%d")
base_url = base + day

Z = '/Z' # session info (base + day + session)
F = '/F' # event info (base + day + session + event)
transaction_value = '/combinaisons' # transaction_value (base + day + session + event + transaction_value + (sepc_off or spec_in))
return_rates = '/return_rates/option_type_B?speciale=####'
return_rates_int = '/return_rates/option_type_B_INTERNATIONAL?speciale=####'

base_path = config['program_file_directories']['base_target']
transaction_value_data_main_path = base_path + config['program_file_directories']['daily_transaction_value_data'] + config['program_file_directories']['transaction_value_data']
return_rate_data_main_path = base_path + config['program_file_directories']['daily_transaction_value_data'] + config['program_file_directories']['return_rate_data']
available_flag_data_main_path = base_path + config['program_file_directories']['daily_transaction_value_data'] + config['program_file_directories']['available_flag_data']

transaction_value_data_path = transaction_value_data_main_path + day_db_format + "/"
return_rate_data_path = return_rate_data_main_path + day_db_format + "/"
available_flag_data_path = available_flag_data_main_path + day_db_format + "/"

if not os.path.exists(transaction_value_data_path):
    os.makedirs(transaction_value_data_path)    
if not os.path.exists(return_rate_data_path):
    os.makedirs(return_rate_data_path)
if not os.path.exists(available_flag_data_path):
    os.makedirs(available_flag_data_path)

timing_list = pd.read_csv(base_path + 'service_program' + "/" + 'timing_list.csv')

#%%

class MyThread(threading.Thread):
    def __init__(self, k, event_list, timing_list):
        super().__init__()
        
        self.day_service_format = datetime.strftime(datetime.today(), "%d%m%Y")
        self.day_db_format = datetime.strftime(datetime.today(), "%Y%m%d")
        
        self.fetch_logger = create_logging(config['log_stages']['threads'])
        self.analyzer_logger = create_logging(config['log_stages']['analyse'])

        self.data_lock = threading.Lock()
        
        self.df_transaction_value_combined = pd.DataFrame()
        self.df_return_rate_combined = pd.DataFrame()
        
        self.i = (k - 1) / 2 if k % 2 == 1 else k / 2
        self.ii = (k - 1) / 2 if k % 2 == 1 else None
        self.iii = k / 2 if k % 2 == 0 else None
        
        self.event_list = event_list
        self.t = timing_list
        
        self.h = self.event_list.loc[self.i, 'start_time']
        self.r = self.event_list.loc[self.i, 'Z']
        self.c = self.event_list.loc[self.i, 'F']
        self.a = self.event_list.loc[self.i, 'observation_type']
        self.o = self.event_list.loc[self.i, 'Online']
        if self.event_list.loc[self.i, 'typeoperation'] == 'option_type_B_INTERNATIONAL':
            self.p = 1
        else:
            self.p = 0

    def save_to_csv(self):
        with self.data_lock:
            if not self.df_transaction_value_combined.empty:
                self.df_transaction_value_combined.to_csv(f"{transaction_value_data_path}Z{self.df_transaction_value_combined.loc[1, 'numerosession']}F{self.df_transaction_value_combined.loc[1, 'numeroCourse']}.csv")
            if not self.df_return_rate_combined.empty:    
                self.df_return_rate_combined.to_csv(f"{return_rate_data_path}Z{self.r}F{self.c}.csv")
                self.fetch_logger.info(f"Data for event Z{self.df_transaction_value_combined.loc[1, 'numerosession']}F{self.df_transaction_value_combined.loc[1, 'numeroCourse']} saved.")

    def run(self):
        random_sleep_interval = random.uniform(0, config['thread_config']['run_random_sleep_interval'])
        time.sleep(random_sleep_interval / 1000)

        if self.ii is not None:
            if self.r == 99:
                self.analyzer_logger.info(f"Dummy_{self.i}")
            if self.a == 'LOCAL':
                self.analyzer_logger.info(f"Local event Z{self.r}_F{self.c}")
            else:
                self.analyzer_logger.info(f"start thread_{self.i} Z{self.r}_F{self.c}")
                self.analyzer_func()
                self.analyzer_logger.info(f"end thread_{self.i} Z{self.r}_F{self.c}")
        if self.iii is not None:
            if self.r == 99:
                self.fetch_logger.info(f"Dummy_{self.i}")
            if self.a == 'LOCAL':
                self.fetch_logger.info(f"Local event Z{self.r}_F{self.c}")
            else:
                self.fetch_logger.info(f"start thread_{self.i} Z{self.r}_F{self.c}")
                self.event_func()
                self.fetch_logger.info(f"end thread_{self.i} Z{self.r}_F{self.c}") 

#######################################################

    def analyzer_func(self):
        after = self.h - config['thread_config']['analyzer_default_starting_time']
        if self.h - config['thread_config']['analyzer_to_late_time'] > now and self.o == False and self.a != 'LOCAL':
            s = sched.scheduler(time.time, time.sleep)
            s.enterabs(after, 0, self.analyzer_main_func)
            s.run()
        else:
            self.fetch_logger.info(f"event already started Z{self.r}_F{self.c}")

    def analyzer_main_func(self):
        retry(lambda: fetch_entity_data(self.event_list, self.day_service_format, self.day_db_format, self.i, daily_data = True), retries = config['fetch_config']['fetch_second_data_retries'], delay = config['fetch_config']['fetch_second_data_delay'])
        
#######################################################
            
    def event_func(self):
        after = self.h - config['thread_config']['fetch_default_starting_time']
        if self.h - config['thread_config']['fetch_to_late_time'] > now and self.o == False and self.a != 'LOCAL':
            s = sched.scheduler(time.time, time.sleep)
            s.enterabs(after, 0, self.fetch_main_func)
            s.run()
        else:
            self.fetch_logger.info(f"event already started Z{self.r}_F{self.c}")
            
    def fetch_main_func(self):
        for j in range (len(self.t)-1):
            self.fetch_int_dist_func(j)
            
    def fetch_int_dist_func(self, j):
        self.fetch_logger.info(f"time interval {self.t.loc[j, 'interval']}_Z{self.r}_F{self.c}")
        if len(self.t) > j+3:
            interval = self.h + self.t.loc[j, 'interval']
            s1 = sched.scheduler(time.time, time.sleep)
            s1.enterabs(interval, 0, self.fetch_func, argument=(j, ))
            s1.run()
        elif len(self.t) > j+2:
            interval = self.h + self.t.loc[j, 'interval']
            s2 = sched.scheduler(time.time, time.sleep)
            s2.enterabs(interval, 0, self.fetch_available_flag_func, argument=(j, ))
            s2.run()
        else:
            interval = self.h + self.t.loc[j+1, 'interval']
            s3 = sched.scheduler(time.time, time.sleep)
            s3.enterabs(interval, 0, self.fetch_last_func)
            s3.run()
            
    def fetch_func(self, j):
        while time.time() < self.h + self.t.loc[j+1, 'interval']:
            update_time_1 = time.time()
            self.quoten_fetch_func()
            update_time_2 = time.time()
            if self.h - update_time_2 < config['thread_config']['save_csv_file']:
                self.save_to_csv()
            if update_time_2 - update_time_1 > self.t.loc[j ,'repetition']:
                pass
            else:
                wait = self.t.loc[j ,'repetition'] - (update_time_2 - update_time_1)
                random_sleep_interval = random.uniform(0, config['thread_config']['fetch_random_sleep_interval'])
                time.sleep(wait + random_sleep_interval/1000)

    def fetch_available_flag_func(self, j):
        available_flag = "True"
        while available_flag == "True":
            update_time = time.time()
            self.quoten_fetch_func()
            if self.h - update_time < config['thread_config']['save_csv_file']:
                self.save_to_csv()
            try:
                # with open("headers.yml") as f_headers:
                #     browser_headers = yaml.safe_load(f_headers)
                # browser_headers["Firefox"]
                content_event_raw = requests.get(f"{base_url}{Z}{self.r}{F}{self.c}")#, proxies=proxies)
                content_event = content_event_raw.json()
                content_event_raw.close()
            except Exception as e:
                self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
                pass
            try:
                df_event = pd.json_normalize(content_event, record_path = [config['thread_config']['available_flag_record_path']])
                df_event.to_csv (f"{available_flag_data_path}Z{self.r}F{self.c}_{update_time}.csv")
                available_flag = str(df_event.loc[0, 'available_flag'])
                self.fetch_logger.info(f"{available_flag}_Z{self.r}_F{self.c}")
            except Exception as e:
                self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
                pass
            update_time_2 = time.time()
            if update_time_2 - update_time > self.t.loc[j ,'repetition']:
                pass
            else:
                wait = self.t.loc[j ,'repetition'] - (update_time_2 - update_time)
                random_sleep_interval = random.uniform(0, config['thread_config']['fetch_random_sleep_interval'])
                time.sleep(wait + random_sleep_interval/1000)
                
    def fetch_last_func(self):
        random_sleep_interval = random.uniform(config['thread_config']['fetch_last_under_random_sleep_interval'], config['thread_config']['fetch_last_upper_random_sleep_interval'])
        time.sleep(random_sleep_interval/1000)
        self.quoten_fetch_func()
        self.save_to_csv()
        self.fetch_logger.info(f"fetch_last_Z{self.r}_F{self.c} and thread_{self.i} closed!")

    def quoten_fetch_func(self):
        update_time = time.time()
        update_time_rounded = round(update_time)
        self.fetch_logger.info(f"quoten_fetch_func Z{self.r}_F{self.c}")
        try:
            # with open("headers.yml") as f_headers:
            #     browser_headers = yaml.safe_load(f_headers)
            # browser_headers["Firefox"]
            content_transaction_value_raw = requests.get(f"{base_url}{Z}{self.r}{F}{self.c}{transaction_value}")#, proxies=proxies)
            content_transaction_value = content_transaction_value_raw.json()
            content_transaction_value_raw.close()
        except Exception as e:
            self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
            pass
        try:
            df_transaction_value = pd.json_normalize(content_transaction_value, record_path = [config['thread_config']['transaction_value_record_path']], meta = ["dateProgramme", "numerosession", "numeroCourse"])
            df_transaction_value_Q = df_transaction_value.explode('listeCombinaisons').reset_index()
            df_transaction_value_Q['collected_at'] = update_time_rounded
            with self.data_lock:  # Ensure thread-safe access
                self.df_transaction_value_combined = pd.concat([self.df_transaction_value_combined, df_transaction_value_Q], ignore_index=True)

        except Exception as e:
            self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
            pass
        try:
            # with open("headers.yml") as f_headers:
            #     browser_headers = yaml.safe_load(f_headers)
            # browser_headers["Firefox"]
            if self.p == 0:
                return_rates_path = return_rates
            elif self.p == 1:
                return_rates_path = return_rates_int
            content_return_rates_raw = requests.get(f"{base_url}{Z}{self.r}{F}{self.c}{return_rates_path}")#, proxies=proxies)
            content_return_rates = content_return_rates_raw.json()
            content_return_rates_raw.close()
        except Exception as e:
            self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
            pass
        try:
            if content_return_rates:
                df_return_rate = pd.json_normalize(content_return_rates, 'return_ratesentity', ['dateMajDirect', 'totaltransaction_value', 'typeoperation', 'numeroCourse', 'numerosession'])
                df_return_rate['collected_at'] = update_time_rounded
            with self.data_lock:
                self.df_return_rate_combined = pd.concat([self.df_return_rate_combined, df_return_rate], ignore_index=True)
        except Exception as e:
            self.fetch_logger.error(f"{e} Z{self.r}_F{self.c}")
            pass                     