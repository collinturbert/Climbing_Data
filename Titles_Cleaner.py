import os
import ast
import logging
import datetime
import pandas as pd
from decouple import config
from sqlalchemy import create_engine, text

### CONFIGURATION ###

# Log Folder
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Titles Cleanup ({datetime.date.today()}).log'

# Batch Processing
BATCH_SIZE = 100000  # Had issues inserting larger df so using batches

# MySQL Database Configuration
DB_CONFIG = {
    "host": config('AWS_HOST'),
    "user": config('AWS_USERNAME'),
    "password": config('AWS_MASTER_PASSWORD'),
    "database": config('AWS_DATABASE')
}


### FUNCTIONS ###

# Initialize the logging system (debug, info, warning, error, and critical)
def setup_logging():
    try:
        os.makedirs(LOG_FOLDER, exist_ok=True)
        log_file = os.path.join(LOG_FOLDER, LOG_FILENAME)
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info(f'Log file created/accessed at {datetime.datetime.now()}')

    except Exception as e:
        print(f'Error creating log file: {e}')


# Create MySQL database connection
def connect_to_db():
    try:
        # Create an SQLAlchemy engine
        engine = create_engine(
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

        # Connect to the engine
        conn = engine.connect()
        logging.info(f'Connected to database at {datetime.datetime.now()}')

        return conn

    except Exception as e:
        logging.error(f'Error connecting to database: {e}', exc_info=True)
        raise


# Drop tables if they exist to enable batch processing with append
def drop_table(conn):
    try:
        drop_statements = [
            text("DROP TABLE IF EXISTS route_descriptions;"),
            text("DROP TABLE IF EXISTS route_directions;"),
            text("DROP TABLE IF EXISTS route_protection;"),
            text("DROP TABLE IF EXISTS route_misc;")]

        for statement in drop_statements:
            conn.execute(statement)

    except Exception as e:
        logging.error(f'Error connecting to database: {e}', exc_info=True)
        raise


# Insert data into each table in the database in batches
def insert_data(conn, table_name, table_data):
    try:
        # Execute MySQL query in batches
        for i in range(0, len(table_data), BATCH_SIZE):
            batch = table_data.iloc[i:i + BATCH_SIZE]
            batch.to_sql(name=table_name, con=conn.engine,
                         if_exists='append', index=False)

    except Exception as e:
        logging.error(f'Error occurred while inserting data into {table_name} table: {e}', exc_info=True)
        conn.rollback()
        raise


# Grab route urls to process from database (only processes route pages)
def get_titles(conn):
    try:
        # Execute MySQL query
        urls_query = """SELECT page_id, description, protection, directions, misc
                                                            FROM mp_route_info"""

        df = pd.read_sql(urls_query, conn)
        logging.info(f'Got dataframe with title values at {datetime.datetime.now()}')

        return df

    except Exception as e:
        logging.error(f'Error getting titles from database: {e}', exc_info=True)
        raise


def process_row(row):
    try:
        # Create output dictionary with titles
        output_dictionary = {'description': '', 'protection': '', 'directions': '', 'misc': ''}

        for key in output_dictionary:
            title_list = ast.literal_eval(row.loc[key])
            output_list = []
            for title_data in title_list:
                values = title_data.split(' - ')
                title_value = values[0].encode('utf-16', 'surrogatepass').decode('utf-16')
                text_value = " - ".join(values[1:]).encode('utf-16', 'surrogatepass').decode('utf-16')
                output_list.append((row.loc['page_id'], title_value, text_value))
            output_dictionary[key] = output_list

        return output_dictionary

    except Exception as e:
        logging.error(f'Error processing titles: {e}', exc_info=True)
        raise


# Main execution
def main():
    # Setup logging and connection to database
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Delete tables if they exit
            drop_table(conn)

            # Get route descriptions
            title_df = get_titles(conn)

            # Process each row for description, protection, directions, and misc
            titles_ml = title_df.apply(process_row, axis=1).to_list()

            # Create empty lists and add the corresponding category data to them
            description_list = []
            protection_list = []
            directions_list = []
            misc_list = []

            for titles_dict in titles_ml:
                description_list.extend(titles_dict['description'])
                protection_list.extend(titles_dict['protection'])
                directions_list.extend(titles_dict['directions'])
                misc_list.extend(titles_dict['misc'])

            # Convert the lists to dataframes for output
            description_df = pd.DataFrame(description_list,
                                          columns=['page_id', 'title', 'text'])

            protection_df = pd.DataFrame(protection_list,
                                         columns=['page_id', 'title', 'text'])

            directions_df = pd.DataFrame(directions_list,
                                         columns=['page_id', 'title', 'text'])

            misc_df = pd.DataFrame(misc_list, columns=['page_id', 'title', 'text'])

            # Insert the data into the MySQL database
            insert_data(conn, 'route_descriptions', description_df)
            insert_data(conn, 'route_protection', protection_df)
            insert_data(conn, 'route_directions', directions_df)
            insert_data(conn, 'route_misc', misc_df)

    except Exception as e:
        logging.error(f'Error occurred in main function:', exc_info=True)
        print(f'Error occurred in main function: {e}')

    finally:
        print('Done!')


### Run ###
if __name__ == '__main__':
    main()
