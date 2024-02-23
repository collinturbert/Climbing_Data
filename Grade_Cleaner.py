import os
import logging
import datetime
import pandas as pd
from decouple import config
from sqlalchemy import create_engine

### CONFIGURATION ###

# Log Folder
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Grade Cleanup ({datetime.date.today()}).log'

# MySQL Database Configuration
DB_CONFIG = {
    "host": config('AWS_HOST'),
    "user": config('AWS_USERNAME'),
    "password": config('AWS_MASTER_PASSWORD'),
    "database": config('AWS_DATABASE')}


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
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

        # Connect to the engine and return it
        conn = engine.connect()
        logging.info(f'Connected to database at {datetime.datetime.now()}')

        return conn

    except Exception as e:
        logging.error(f'Error connecting to database: {e}', exc_info=True)

    return None


# Grab route urls to process from database (only processes route pages)
def get_grades(conn):
    try:
        # Execute MySQL query
        urls_query = "SELECT page_id, long_grade FROM mp_route_info"
        df = pd.read_sql(urls_query, conn)
        logging.info(f'Got list of grades at {datetime.datetime.now()}')

        return df

    except Exception as e:
        logging.error(f'Error getting grades info from database: {e}', exc_info=True)

    return None


# Insert data into each table in the database
def insert_data(conn, grade_data, table_name='route_grades'):
    try:
        # Execute MySQL query
        grade_data.to_sql(name=table_name, con=conn.engine, if_exists='replace', index=False)

    except Exception as e:
        logging.error(f'Error inserting data into database: {e}', exc_info=True)

    return None


# Cleanup long grade for processing
def long_grade_cleanup(df):
    try:
        patterns_1 = {'YDS.*?British': ' ', 'YDS.*?Font': ' '}
        patterns_2 = {
            '5.10 ': '5.10b ',
            '5.11 ': '5.11b ',
            '5.12 ': '5.12b ',
            '5.13 ': '5.13b ',
            '5.14 ': '5.14b ',
            '5.15 ': '5.15b '}

        df['long_grade'] = df['long_grade'].replace(patterns_1, regex=True)
        df['long_grade'] = df['long_grade'].replace(patterns_2, regex=True)

    except Exception as e:
        logging.error(f'Error cleaning up patterns in long grade: {e}')


# Process each row of the dataframe and add values to the appropriate grades column
def process_row(index, row, grades_df, columns, cat_df):
    try:
        grade_values = row['long_grade']
        for col in columns:
            matching_values = [value for value in cat_df[col].dropna() if
                               grade_values.find(value) != -1]

            if len(matching_values) > 0:
                grades_df.at[index, col] = (cat_df.loc[cat_df[col].
                                            isin(matching_values), col].
                                            iloc[-1].
                                            replace('V-easy', 'V Easy').
                                            replace('+', 'd').
                                            replace('-', 'a'))

    except Exception as e:
        logging.error(f'Error connecting grade with grade type: {e}')


# Main execution
def main():
    # Setup logging and connection to database
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Get route long grades and a list of grade categories
            grades_df = get_grades(conn)
            cat_df = pd.read_csv('grade_categories.csv')

            # Cleanup long_grades for processing
            long_grade_cleanup(grades_df)

            # Create grade columns in the grades DataFrame
            columns = ['Rock', 'Boulder', 'Aid', 'Ice', 'Mixed', 'Snow', 'Danger']
            for col in columns:
                grades_df[col] = ''

            # Iterate through long_grade and fill in values for the grade columns
            grades_df.apply(lambda row: process_row(row.name, row, grades_df, columns, cat_df), axis=1)

            # Melt grades df
            columns_to_keep = ['page_id']
            columns_to_melt = ['Rock', 'Boulder', 'Aid', 'Ice', 'Mixed', 'Snow', 'Danger']
            melted_df = pd.melt(grades_df, id_vars=columns_to_keep, value_vars=columns_to_melt,
                                var_name='Type', value_name='Grade')
            melted_df = melted_df[melted_df['Grade'] != '']
            melted_df.reset_index(drop=True, inplace=True)

            # Insert grade data into database
            insert_data(conn, melted_df)

    except Exception as e:
        logging.error(f'Error occured in main function:', exc_info=True)
        print(f'Error occured in main function: {e}')

    finally:
        # Close connection to database
        conn.close()


### Run ###
if __name__ == '__main__':
    main()
