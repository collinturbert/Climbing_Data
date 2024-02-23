import os
import logging
import datetime
import pandas as pd
from decouple import config
from sqlalchemy import create_engine
import Stats_Grabber as stats_grabber

### CONFIGURATION ###

# Log Folder
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Data Check ({datetime.date.today()}).log'

# MySQL Database Configuration
DB_CONFIG = {
    "host": config('AWS_HOST'),
    "user": config('AWS_USERNAME'),
    "password": config('AWS_MASTER_PASSWORD'),
    "database": config('AWS_DATABASE'), }

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
        print(f'Error creating log folder: {e}')


# Create MySQL database connection
def connect_to_db():
    try:
        # Create an SQLAlchemy engine
        engine = create_engine(
            f"""mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/
                {DB_CONFIG['database']}""")

        # Connect to the engine and create a cursor
        conn = engine.connect()
        logging.info(f'Connected to database at {datetime.datetime.now()}')

        return conn

    except Exception as e:
        logging.error(f'Error connecting to database: {e}', exc_info=True)

    return None


# Insert data into each table in the database
def insert_data(conn, stats_check):
    try:
        # Execute MySQL query
        table_name = 'stats_check'
        stats_check.to_sql(name=table_name, con=conn.engine, if_exists='replace', index=False)
        logging.info('Successfully added data to the database')

    except Exception as e:
        logging.error(f'Error inserting data into database: {e}', exc_info=True)

    return None


# Gret stats data to process from database
def get_stats(conn):
    try:
        # Queries to execute
        counts_query = "SELECT * FROM stats_count"
        ratings_query = "SELECT page_id, COUNT(page_id) AS 'ratings_count' FROM stats_ratings GROUP BY page_id"
        stars_query = "SELECT page_id, COUNT(page_id) AS 'stars_count' FROM stats_stars GROUP BY page_id"
        ticks_query = "SELECT page_id, COUNT(page_id) AS 'ticks_count' FROM stats_ticks GROUP BY page_id"
        todos_query = "SELECT page_id, COUNT(page_id) AS 'todos_count' FROM stats_todos GROUP BY page_id"

        # Create dataframes
        logging.info('Starting to get ready to get data')
        counts_df = pd.read_sql(counts_query, conn)
        logging.info('got counts_df')
        ratings_df = pd.read_sql(ratings_query, conn)
        logging.info('got ratings_df')
        stars_df = pd.read_sql(stars_query, conn)
        logging.info('got stars_df')
        ticks_df = pd.read_sql(ticks_query, conn)
        logging.info('got ticks_df')
        todos_df = pd.read_sql(todos_query, conn)
        logging.info('got todos_df')
        logging.info(f'Got all five stats dataframes at {datetime.datetime.now()}')

        # Combine dataframes
        dfs_to_join = [ratings_df, stars_df, ticks_df, todos_df]
        for df in dfs_to_join:
            counts_df = counts_df.merge(df, on='page_id', how='left')

        logging.info(f'Combined all stats dataframes at {datetime.datetime.now()}')

        return counts_df

    except Exception as e:
        logging.error(f'Error creating dfs from database: {e}', exc_info=True)

    return None


def process_stats(df):
    try:
        # Create new df that calculates the difference b/w stats count and actual counts in the database
        new_df = pd.DataFrame({'page_id': df['page_id'].copy()})
        new_df['stars_difference'] = df['stars'] - df['stars_count']
        new_df['ratings_difference'] = df['ratings'] - df['ratings_count']
        new_df['ticks_difference'] = df['ticks'] - df['ticks_count']
        new_df['todos_difference'] = df['todos'] - df['todos_count']

        # Calculate the 'sum_difference' column as the sum of the other differences
        new_df['sum_difference'] = new_df[
            ['stars_difference', 'ratings_difference', 'ticks_difference', 'todos_difference']].sum(axis=1).astype('int')

        logging.info(f'Processed stats_check_df at {datetime.datetime.now()}')

        return new_df

    except Exception as e:
        logging.error(f'Error creating stats_check_df: {e}', exc_info=True)

    return None


# Main execution
def main():
    # Setup logging and connection to database
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Get stats df
            stats_df = get_stats(conn)
            stats_df.to_csv('stats_data.csv', index=False)

            # Create checks dataframe
            stats_check_df = process_stats(stats_df)
            stats_check_df.to_csv('stats_check.csv', index=False)
            stats_check_df.head()

            page_id_df = stats_check_df['sum_difference' > 1]
            page_id_df.to_csv('page_ids.csv')

            # Insert grade data into database
            insert_data(conn, stats_check_df)

    except Exception as e:
        logging.error(f'Error occurred in main function:', exc_info=True)
        print(f'Error occurred in main function: {e}')


### Run ###
if __name__ == '__main__':
    main()
