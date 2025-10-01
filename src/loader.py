#%% notes

# !!! add logger information !!!

#%% modules
import os
os.chdir(r'C:\Projekt\service\2_program\service_program')

import pandas as pd
import numpy as np

import psycopg2

from my_functions import load_yaml, create_logging

config = load_yaml('config')
dictionary = load_yaml('dictionary')
#%% functions

class DatabaseConnection():
    def __init__(self, config):
        self.config = config
        self.connection = None
        
    def connect(self):
        if not self.connection:
            self.connection = psycopg2.connect(
                host = self.config['database_config']['host'],
                dbname = self.config['database_config']['database_name'],
                user = self.config['database_config']['user'],
                password = self.config['database_config']['password'],
                port = self.config['database_config']['port']
            )
    
    def get_cursor(self):
        if not self.connection:
            raise psycopg2.OperationalError("No active database connection. Call connect() first.")
        return self.connection.cursor()
    
    def db_commit(self):
        """Commits the current transaction to the database."""
        if self.connection:
            self.connection.commit()
        else:
            raise psycopg2.OperationalError("No active database connection. Call connect() first.")
        
    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

#%% main functions

# Function to read data from the database
def fetch_data_from_db(query, config):
    """
    Execute a SQL query and fetch data from the database.

    Parameters:
    - query (str): The SQL query to execute.
    - config (dict): Configuration dictionary for database connection.

    Returns:
    - pd.DataFrame: DataFrame containing the fetched results.
    """
    db = DatabaseConnection(config)
    db.connect()
    try:
        with db.get_cursor() as cursor:
            cursor.execute(query)
            # Fetch all rows from the result set
            rows = cursor.fetchall()
            # Get column names from cursor
            col_names = [desc[0] for desc in cursor.description]
        # Convert the result set into a Pandas DataFrame
        return pd.DataFrame(rows, columns=col_names)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure
    finally:
        db.close()
        
def get_column_data_types(table_name):
    db = DatabaseConnection(config)
    db.connect()
    """
    Fetch the column names and their respective data types from the database table.
    
    Parameters:
    - db_connection (DatabaseConnection): A database connection object.
    - table_name (str): The name of the table to query.
    
    Returns:
    - dict: A dictionary with column names as keys and their data types as values.
    """
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
    """
    with db.get_cursor() as cursor:
        cursor.execute(query)
        columns = cursor.fetchall()
    
    column_data_types = {column[0]: column[1] for column in columns}
    return column_data_types

def insert_data(df, target_table, target_columns, day_db_format, primary_key_column = None):
    logger = create_logging(config['log_stages']['loader'])
    db = DatabaseConnection(config)
    db.connect()
    
    # DataFrame to collect all inserted rows
    inserted_data = pd.DataFrame([]) if primary_key_column else None
    
    """
    Inserts data into the specified target table with type handling for NaN and date formats.
    
    Parameters:
    - df (pd.DataFrame): The DataFrame containing data to be inserted.
    - target_table (str): The database table to insert data into.
    - target_columns (list): The list of columns in the target table to populate.
    - primary_key_column (str, optional): The name of the primary key column to return. If None, no keys are returned.

    Returns:
    - pd.DataFrame: DataFrame with original columns and the primary key values, if provided. Otherwise, returns None.
    """
    
    with db.get_cursor() as cursor:
        cursor.execute("SAVEPOINT before_row")
        for index, row in df.iterrows():
            try:
                # Filter row data to include only target_columns present in df
                filtered_values = [
                    None if pd.isna(row[column]) else row[column]
                    for column in target_columns if column in df.columns
                ]
                filtered_columns = [column for column in target_columns if column in df.columns]
    
                # Construct the INSERT query 
                insert_query = f"""
                INSERT INTO {target_table} ({', '.join(filtered_columns)})
                VALUES ({', '.join(['%s' for _ in filtered_columns])})
                """
    
                
                casted_values = [
                    val.strftime('%Y-%m-%d %H:%M:%S') if isinstance(val, pd.Timestamp) else val
                    for val in filtered_values
                ]
    
                cursor.execute(insert_query, tuple(casted_values))
                
                #primary key if specified
                primary_key_value = None
                if primary_key_column:
                    primary_key_query = f"SELECT currval(pg_get_serial_sequence('{target_table}', '{primary_key_column}'))"
                    cursor.execute(primary_key_query)
                    primary_key_value = cursor.fetchone()[0]


                original_row = row.to_dict()
                if primary_key_column:
                    original_row[primary_key_column] = primary_key_value
                    
                inserted_data = pd.concat([inserted_data, pd.DataFrame([original_row])], ignore_index=True)
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT before_row")
                logger.error(f"data {day_db_format} for {filtered_columns[0]}: {filtered_values[0]} in {target_table} not integrated: {e}.")
            finally:
                cursor.execute("SAVEPOINT before_row")  # Reset savepoint
        
        db.connection.commit()
        db.close()
        logger.info(f"data {day_db_format} for {target_table} successfully integrated.")
        
        # Return the DataFrame if a primary key is requested; otherwise, None        
        return inserted_data if primary_key_column else None
 
def bulk_data(df, target_table, target_columns, batch_size=10000):
    """
    Inserts data into the specified target table using PostgreSQL's COPY for bulk data loading,
    ensuring data type alignment with the database schema.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing data to be inserted.
    - target_table (str): The database table to insert data into.
    - target_columns (list): The list of columns in the target table to populate.
    - primary_key_column (str, optional): The name of the primary key column to return. If None, no keys are returned.
    - batch_size (int): The number of rows to include in each batch during insertion.

    Returns:
    - pd.DataFrame: DataFrame with original columns and the primary key values, if provided. Otherwise, returns None.
    """
    import io
    logger = create_logging(config['log_stages']['loader'])
    db = DatabaseConnection(config)

    # Step 1: Fetch column data types from the database
    def get_column_data_types():
        query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{target_table}';
        """
        db.connect()
        with db.get_cursor() as cursor:
            cursor.execute(query)
            columns = cursor.fetchall()
        db.close()
        return {col[0]: col[1] for col in columns}

    column_data_types = get_column_data_types()

    # Step 2: Map PostgreSQL types to Pandas types
    pg_to_pd_type_map = {
        'integer': 'Int64',  # Pandas nullable integer
        'bigint': 'Int64',
        'smallint': 'Int64',
        'numeric': 'float64',
        'double precision': 'float64',
        'text': 'object',
        'character varying': 'object',
        'timestamp without time zone': 'datetime64[ns]',
        'timestamp with time zone': 'datetime64[ns]',
        'date': 'datetime64[ns]'
    }

    # Step 3: Align DataFrame types with the database
    for column in target_columns:
        if column in column_data_types:
            pg_type = column_data_types[column]
            if pg_type in pg_to_pd_type_map:
                try:
                    df[column] = df[column].astype(pg_to_pd_type_map[pg_type])
                except Exception as e:
                    logger.error(f"Error converting column {column} to type {pg_to_pd_type_map[pg_type]}: {e}")

    # Step 4: Insert data in batches using COPY
    total_rows = len(df)
    for batch_start in range(0, total_rows, batch_size):
        # Get a batch of rows
        batch = df.iloc[batch_start:batch_start + batch_size]

        # Convert the DataFrame batch to CSV format in-memory
        csv_buffer = io.StringIO()
        batch[target_columns].to_csv(csv_buffer, index=False, header=False)  # No header, as it's unnecessary for COPY
        csv_buffer.seek(0)  # Move the buffer cursor to the beginning

        try:
            db.connect()
            with db.get_cursor() as cursor:
                # Use COPY command to insert data from the CSV buffer
                cursor.copy_expert(
                    f"""
                    COPY {target_table} ({', '.join(target_columns)}) 
                    FROM STDIN WITH CSV
                    """, csv_buffer
                )
                db.connection.commit()
                logger.info(f"Batch {batch_start // batch_size + 1} successfully inserted into {target_table}.")
        except Exception as e:
            db.connection.rollback()
            logger.error(f"Error inserting batch {batch_start // batch_size + 1} into {target_table}: {e}.")
        finally:
            db.close()
                       
class ForeignKeyResolver:
    def __init__(self):
        self.foreign_keys_config = {}
        self.db = DatabaseConnection(config)
        self.db.connect()
        self.logger = create_logging(config['log_stages']['loader'])
        
    def add_foreign_key(self, key):
        """
        Adds a foreign key configuration for resolving and potentially inserting new records.
        
        Parameters:
        - primary_key (str): The primary_key in the target dataframe representing the foreign key (e.g., 'country_id').
        - reference_table (str): The reference table where the foreign key resides (e.g., 'countries').
        - match_attributes (list of str): The list of columns in the reference table to match existing records.
        - insert_attributes (dict): The dictionary specifying columns and values to insert if the foreign key doesn't exist.
        """
        
        primary_key = key['primary_key']
        reference_table = key['reference_table']
        match_attributes = key['match_attributes']
        insert_attributes = key.get('insert_attributes', None)  # Default to None if not provided
        
        self.foreign_keys_config[primary_key] = {
            'reference_table': reference_table,
            'match_attributes': match_attributes,
            'insert_attributes': insert_attributes
        }

    def get_or_create_foreign_key(self, cursor, primary_key, row):
        """
        Resolves a foreign key for a specific row by either retrieving or creating the record in the reference table.
        """
        config = self.foreign_keys_config[primary_key]
        reference_table = config['reference_table']
        match_attributes = {attr: row[attr] for attr in config['match_attributes']}
        insert_attributes = {key: row[val] for key, val in (config['insert_attributes'] or {}).items()}

        # Prepare query to check if the foreign key exists
        query = f"SELECT {primary_key} FROM {reference_table} WHERE " + " AND ".join([f"{k} = %s" for k in match_attributes.keys()])
        cursor.execute(query, tuple(match_attributes.values()))
        result = cursor.fetchone()
        if insert_attributes is None:
            return None
        else:
            # If the foreign key doesn't exist, insert the missing data
            if not result:
                if insert_attributes:
                    insert_query = f"INSERT INTO {reference_table} ({', '.join(insert_attributes.keys())}) VALUES ({', '.join(['%s' for _ in insert_attributes])}) RETURNING {primary_key}"
                    cursor.execute(insert_query, tuple(insert_attributes.values()))
                    # Commit after inserting to save changes
                    self.db.connection.commit()
                    return cursor.fetchone()[0]  # Return the new ID
                # else:
                #     print(f"Error: Missing insert attributes for {reference_table}")
                #     self.logger.error(f"Missing insert attributes for {reference_table} and foreign_key {primary_key} not found!")
                #     return None
        if result:
            return result[0]  # Return the existing ID
        else:
            return np.nan
    
    def resolve_foreign_keys(self, df):
        """
        Resolves all foreign keys in the dataframe according to the configuration.
        
        Parameters:
        - df (DataFrame): The dataframe where foreign keys need to be resolved.

        Returns:
        - DataFrame: Updated dataframe with resolved foreign key values.
        """
        with self.db.get_cursor() as cursor:
            for primary_key, config in self.foreign_keys_config.items():
                for index, row in df.iterrows():
                    # Check if all match_attributes are non-NaN
                    if any(pd.isna(row[attr]) for attr in config['match_attributes']):
                        # If any match attribute is NaN, leave the foreign key as NaN and skip
                        df.at[index, primary_key] = np.nan
                        # self.logger.info(f"Cannot create a foreign_key {primary_key} and leave the foreign_key as NaN because match attributes not avaible")
                    else:
                        # Otherwise, resolve or create the foreign key
                        foreign_key_id = self.get_or_create_foreign_key(cursor, primary_key, row)
                        df.at[index, primary_key] = foreign_key_id
        self.db.close() 
        return df  
    
def resolver(dictionary, raw_data):
    resolver = ForeignKeyResolver()
    for key in dictionary:
        resolver.add_foreign_key(key)
    data = resolver.resolve_foreign_keys(raw_data) 
    return data

    