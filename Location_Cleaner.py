import os
import logging
import datetime
import pandas as pd
from decouple import config
from sqlalchemy import create_engine

### CONFIGURATION ###

# Log Folder
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Location Cleanup ({datetime.date.today()}).log'

# MySQL Database Configuration
DB_CONFIG = {
    "host": config('AWS_HOST'),
    "user": config('AWS_USERNAME'),
    "password": config('AWS_MASTER_PASSWORD'),
    "database": config('AWS_DATABASE')}

INSERT_TABLE = 'route_location'


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
        conn = engine.connect()
        logging.info(f'Connected to database at {datetime.datetime.now()}')

        return conn

    except Exception as e:
        logging.error(f'Error connected to db: {e}', exc_info=True)
        raise


# Grab route locations from MySQL database
def get_route_locations(conn):
    query = "SELECT page_id, location FROM mp_route_info"
    try:
        route_location = pd.read_sql(query, conn)
        logging.info(f'Got list of locations at {datetime.datetime.now()}')

        return route_location

    except Exception as e:
        logging.error(f'Error fetching data from the database: {e}', exc_info=True)

    return None


# Split each route location and sort based on values
def location_cleaner(location_column):
    try:
        output_list = []

        location_split = location_column.split(' > ')

        if location_split[0] == 'International':
            continent = location_split[1]
            country = location_split[2]
            state = 'N/A'
            international_output = [continent, country, state]
            for sub_area in location_split[3:]:
                international_output.append(sub_area)
            output_list.extend(international_output)

        else:
            continent = 'N America'
            country = 'United States'
            state = location_split[0]
            us_output = [continent, country, state]
            for sub_area in location_split[1:]:
                us_output.append(sub_area)
            output_list.extend(us_output)

        # Ensure all routes have the same number of values
        while len(output_list) < 11:
            output_list.extend([None])
        while len(output_list) > 11:
            output_list.pop()

        return output_list

    except Exception as e:
        logging.error(f'Error fetching data from the database: {e}', exc_info=True)

    return None


# Main execution
def main():
    # Setup logging and connection to database
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Get route types
            route_locations = get_route_locations(conn)
            logging.info(f'Length of locations_list is {len(route_locations)}')

            # Process each route and create output
            cleaned_locations = route_locations['location'].apply(location_cleaner).apply(pd.Series)
            cleaned_locations.columns = ['continent', 'country', 'state', 'area_1', 'area_2', 'area_3', 'area_4',
                                         'area_5', 'area_6', 'area_7', 'area_8']

            # Combine dataframes without original location column
            locations_df = pd.concat([route_locations['page_id'], cleaned_locations], axis=1)
            logging.info(f'Length of output_list is {len(locations_df)}')

            # Print df information and then add data to MySQL table
            locations_df.describe(include='all').to_csv('locations_df_stats.csv')
            locations_df.to_sql(name=INSERT_TABLE, con=conn.engine, if_exists='replace', index=False)
            logging.info(f'Inserted data of length {len(locations_df)} at {datetime.datetime.now()}')

    except Exception as e:
        logging.error(f'Error occurred in main function:', exc_info=True)
        print(f'Error occurred in main function: {e}')

    finally:
        print('Done!')


### Run ###
if __name__ == '__main__':
    main()
