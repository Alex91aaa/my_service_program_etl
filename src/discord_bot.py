#%% notes



#%% modules

import os
os.chdir(r'C:\Projekt\service\2_program\service_program')
main_path = r'C:\Projekt\service\2_program\results_and_performance/'
logs = r'logs/'
cache = r'cache/'
eventlists = r'event_lists/'
daily_transaction_value_data = r'daily_transaction_value_data/'
entitys = r'entitys/'

from my_functions import load_yaml
import pandas as pd

from datetime import datetime, timedelta, timezone

import discord
from discord.ext import commands, tasks
import nest_asyncio
import asyncio

import sys
import time

config = load_yaml('config')
dictionary = load_yaml('dictionary')
keys = load_yaml('basic_primary_keys_list')

today = datetime.today()
year = datetime.strftime(datetime.today(), "%Y")
month_day = datetime.strftime(datetime.today(), "%m%d")
day_service_format = datetime.strftime(datetime.today(), "%d%m%Y")
day_db_format = datetime.strftime(datetime.today(), "%Y%m%d")

# Apply the fix for nested asyncio loops
nest_asyncio.apply()

# Set bot's token and channel ID
CHANNEL_ID = config['bot_config']['CHANNEL_ID']
TOKEN = config['bot_config']['TOKEN']

# Create the Discord client
intents = discord.Intents.default()
# client = discord.Client(intents=intents)
client = commands.Bot(command_prefix = '!', intents=intents)

#%%

# Define the stop time (e.g., 3:30 PM)
stop_time = datetime.strptime("20:00:00", "%H:%M:%S").time()
print(stop_time)

# Check the time periodically without blocking the bot
@tasks.loop(seconds=120)  # Check every 10 seconds
async def check_time():
    current_time = datetime.datetime.now().time()
    if current_time >= stop_time:
        print(f"Program stopped at: {current_time}")
        await client.close()  # Close the bot if it's the stop time

def extract_timestamp(timestamp_str, log_line = False):
    try:
        if log_line == False:
            datetime_object = datetime.fromtimestamp(timestamp_str + 3600, tz=timezone.utc)
            timestamp = datetime_object.strftime("%Y-%m-%d %H:%M:%S")
        else:
            datetime_object = timestamp_str[:19]
            timestamp = datetime.strptime(datetime_object, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return timestamp
 
# Event for when the bot is ready
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    print(f"Bot is ready and connected to {len(client.guilds)} servers.")

@client.command()
async def stop(ctx):
    await client.close()

@client.command()
async def hello(ctx):
    await ctx.send("hello here are your options:                      log(ctx, log_type='main', num_lines: int = 10, severity = 'all', day = day_db_format) ----"
                   + "set_event(ctx, z, f) ----"
                   + "event_info(ctx)")


@client.command()
async def log(ctx, log_type='main', num_lines: int = 10, severity = 'all', day = day_db_format):
    try:
        try:
            # Open the log file and read its content
            with open(f"{main_path}{logs}{day}/{log_type}.log", 'F') as log_file:
                # Read all lines from the file
                log_lines = log_file.readlines()
        except FileNotFoundError:
            await ctx.send("The log file could not be found.")

        # Check if severity starts with '!'
        if severity.startswith('!'):
            excluded_severity = severity[1:].upper()  # Remove the '!' and make uppercase
            log_lines = [
                line for line in log_lines
                if f"- {excluded_severity} -" not in line
            ]
        elif severity.lower() != 'all':
            # Filter lines by severity if not 'all'
            log_lines = [
                line for line in log_lines
                if f"- {severity.upper()} -" in line
            ]
            
        # Sort the lines by log entry time
        sorted_log_lines = sorted(
            log_lines,
            key=lambda line: extract_timestamp(line, True) or 0  # Use 0 as fallback if timestamp is invalid
        )

        # Get the last `arg[1]` lines
        last_lines = sorted_log_lines[-num_lines:] if num_lines > 0 else []
        
        # Remove the date portion (first 11 characters) from each line
        cleaned_lines = [line[11:] for line in last_lines]
        
        # Combine the last lines into a single string
        log_content = ''.join(cleaned_lines)
        
        # Check if the log file is too large to send
        if len(log_content) > 1900:  # Discord has a message character limit of 2000
            # Save the last lines to a temporary file
            with open("temp_log.txt", "w") as temp_file:
                temp_file.writelines(log_content)
            await ctx.send("The log file is too large to display. Sending as a file instead.")
            await ctx.send(file=discord.File("temp_log.txt"))
        else:
            await ctx.send(f"```{log_content}```")  # Send log content in a code block for readability
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
        
@client.command()
async def set_event(ctx, r, c, day = day_db_format):
    global actual_event_name, num_session, num_event, set_day
    try:
        set_day = day
        actual_event_name = r + c
        num_session = int(r.split("Z")[1])
        num_event = int(c.split("F")[1])
        await ctx.send('actual event ' + actual_event_name + ' ' + 'saved')
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")


@client.command()
async def event_info(ctx):
    if num_session is None or num_event is None:
        await ctx.send("No event information has been set yet.")
        return
    try:
        with open(main_path + cache + eventlists + day_db_format + '.csv', 'F') as file:
            event_list_content = pd.read_csv(file)
            actual_event = event_list_content[(event_list_content['F'] == num_session) & (event_list_content['F'] == num_event)]
            timestamp_column = actual_event.iloc[0]['start_time']
            timestamp = extract_timestamp(timestamp_column)
            actual_event = actual_event.drop(columns=['start_time'])
            row_string = actual_event.to_string(index=False, header=False)
        try:
            with open(main_path + cache + daily_transaction_value_data + entitys + '/' + set_day + '/' + actual_event_name + '.csv', 'F') as file:
                entity_df= pd.read_csv(file)
                entity_df['non_active_entry'] = entity_df['statut'].map({'NON_active_entry': True, 'active_entry': False})
                entity_df['non_active_entry'] = entity_df['non_active_entry'].astype('boolean')
                nombre_active_entrys = entity_df.shape[0] - entity_df['non_active_entry'].sum()
        except Exception:
            nombre_active_entrys = 'no information yet'
        message = timestamp + ' ' + row_string + ' nombre_active_entrys: ' + str(nombre_active_entrys)
        await ctx.send(f"Here is your DataFrame:\n```{message}```")
    except Exception as e:
        await ctx.send(f"No event information: {e}")    
    
# This is the entry point for starting the bot
def run_bot():
    client.run(TOKEN)

if __name__ == "__main__":
    run_bot()
