import os
import time
import json
import logging
import requests
import datetime
import pandas as pd
import concurrent.futures
from statistics import mean
from decouple import config
from sqlalchemy import create_engine, text
from ratelimit import limits, sleep_and_retry


### CONFIGURATION
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Stats Grabber ({datetime.date.today()}).log'
BATCH_SIZE = 100
CALLS_PER_PERIOD = 20
TIME_PERIOD = 10
TABLE_LIST = ['stars', 'ticks', 'todos', 'ratings', 'count']

DB_CONFIG = {
    "host": config('AWS_HOST'),
    "user": config('AWS_USERNAME'),
    "password": config('AWS_MASTER_PASSWORD'),
    "database": config('AWS_DATABASE')
}


### FUNCTIONS ###
# Initialize the logging system
def setup_logging():
    try:
        os.makedirs(LOG_FOLDER, exist_ok=True)
        log_file = os.path.join(LOG_FOLDER, LOG_FILENAME)
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info(f'Log file created/accessed at {datetime.datetime.now()}')

    except Exception as e:
        print(f'Error setting up log file: {e}')


# Create a MySQL database connection
def connect_to_db():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        )
        conn = engine.connect()
        logging.info(f'Connected to the database at {datetime.datetime.now()}')
        return conn

    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')

    return None


# Fetch route URLs to process from the database (only route pages)
def get_page_id(conn):
    try:
        urls_query = text("SELECT page_id FROM mp_urls WHERE INSTR(url, '/route/')")
        page_id = conn.execute(urls_query).fetchall()
        page_id = [value[0] for value in page_id]
        return page_id

    except Exception as e:
        logging.error(f'Error fetching route URLs from the database: {e}')

    return None

# Fetch processed URLs from the database
def get_processed_data(conn):
    try:
        get_query = text('SELECT page_id, date_added FROM stats_count')
        rows = conn.execute(get_query).fetchall()
        processed_page_id = [row[0] for row in rows]
        processed_date_added = [row[1] for row in rows]
        processed_urls = list(zip(processed_page_id, processed_date_added))
        logging.info(f'Got a list of processed routes at {datetime.datetime.now()}')
        return processed_urls

    except Exception as e:
        logging.error(f'Error fetching processed data from the database: {e}')

    return None


# Fetch processed URLs from the database
def drop_rows(page_ids, conn):
    try:
        for table in TABLE_LIST:
            drop_query = text(f'DELETE FROM stats_{table} WHERE page_id IN {tuple(page_ids)}')
            conn.execute(drop_query)

        conn.commit()
        logging.info(f'Dropped {len(page_ids)} page_ids from database')

    except Exception as e:
        logging.error(f'Error dropping rows from the database: {e}')
        print(f'Error dropping rows from the database: {e}')
        raise

# Get grade data for stats_grabber
@sleep_and_retry
@limits(calls=CALLS_PER_PERIOD, period=TIME_PERIOD)
def json_puller(page_id, retry=True):
    stats_output = {}
    stats_count = {}

    try:
        for stat in TABLE_LIST[:4]:
            url = 'https://www.mountainproject.com/api/v2/routes/' + str(page_id) + '/' + stat
            response = requests.get(url)

            if response.status_code == 200:
                json_data = json.loads(response.text)
                data_list = json_data['data']
                stat_total = json_data['total']
                last_page = json_data['last_page']

                if last_page != 1:
                    extra_url_list = [f"{url}?page={i + 1}" for i in range(1, last_page)]
                    for url in extra_url_list:
                        response_2 = requests.get(url)
                        if response_2.status_code == 200:
                            json_data_2 = json.loads(response_2.text)
                            data_list.extend(json_data_2['data'])
                        else:
                            logging.error(f'Need to process {url} still, got code {response.status_code}')
                            print(f'Need to process {url} still, got code {response.status_code}')

                for value in data_list:
                    del value['id']
                    value['page_id'] = page_id

                    # flatten allRatings list for grades
                    if 'allRatings' in value:
                        value['allRatings'] = ', '.join(value['allRatings'])

                    # Take care of missing and False 'user' values in data
                    if 'user' not in value:
                        value['user'] = 'unkown'
                    elif not value['user']:
                        value['user'] = 'unkown'
                    else:
                        value['user'] = value['user']['name']

                if len(data_list) != stat_total:
                    logging.error(f'Stat length didnt add up for {stat} at {url}.')
                    logging.error(f'Got a length of {len(data_list)} but should have been {stat_total}')

            elif response.status_code == 404:
                logging.error(f'Bailed on {stat} for page_id {page_id}: got code {response.status_code}', exc_info=True)
                return {'stars': []}

            elif retry:
                logging.error(f"Error processing {stat} for page_id {page_id}: got code {response.status_code}",
                              exc_info=True)
                time.sleep(5)
                return json_puller(page_id, retry=False)

            else:
                logging.error(f'Retried {stat} for page_id {page_id}: got code {response.status_code}', exc_info=True)
                return {'stars': []}

            # Create dictionary values for each stat
            stats_output[stat] = data_list
            stats_count[stat] = stat_total

        stats_count['date_added'] = datetime.date.today()
        stats_output['count'] = {page_id: stats_count}

        # Handle the one random rating that has different keys
        filtered_ratings = [ratings_dict for ratings_dict in stats_output['ratings'] if 'userId' not in ratings_dict]
        stats_output['ratings'] = filtered_ratings

        return stats_output

    except Exception:
        logging.error(f'Error processing stats for page_id {page_id}:', exc_info=True)


# Get information for each route
def stats_grabber(page_id_list):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(json_puller, page_id) for page_id in page_id_list]
            output_list = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Combine dictionaries for each stat
        if len(output_list) > 1:
            stats_output = output_list[0].copy()

            for other_dict in output_list[1:]:
                for key, value in other_dict.items():
                    if key in stats_output:
                        if isinstance(stats_output[key], list) and isinstance(value, list):
                            stats_output[key].extend(value)
                        if isinstance(stats_output[key], dict) and isinstance(value, dict):
                            stats_output[key].update(value)

                    else:
                        stats_output[key] = value

            # Create dataframes from list of dictionaries
            for key, value in stats_output.items():
                if key == 'count':
                    df = pd.DataFrame.from_dict(value, orient='index')
                else:
                    df = pd.DataFrame(value)
                stats_output[key] = df

            # Cleanup datatypes and extra characters
            stats_output['ticks']['text'] = stats_output['ticks']['text'].str.replace('&middot;', '').str.strip()

            for table in TABLE_LIST[:4]:
                stats_output[table]['createdAt'] = pd.to_datetime(stats_output[table]['createdAt'])
                stats_output[table]['updatedAt'] = pd.to_datetime(stats_output[table]['updatedAt'])

            return stats_output

    except Exception:
        logging.error(f'Error processing batch:', exc_info=True)

    return None


# Main execution
def main():
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Get list of page_ids that need processed
            all_page_ids = get_page_id(conn)
            processed_urls = get_processed_data(conn)

            # Update any route that is over 30 days old
            process_page_ids = []
            drop_page_ids = []
            for route in processed_urls:
                if (datetime.date.today() - route[1]).days < 30:
                    process_page_ids.append(route[0])
                else:
                    drop_page_ids.append(route[0])

            filtered_list = list(set(all_page_ids).difference(process_page_ids))
            print(f'Processing list of {len(filtered_list)} page_id')

            # Drop rows for page_id's that need updated
            if len(drop_page_ids) > 0:
                drop_rows(drop_page_ids, conn)

            # Create batches and process them
            batch_times = []

            for i in range(0, len(filtered_list), BATCH_SIZE):
                start_time = time.time()
                route_batch = filtered_list[i:i + BATCH_SIZE]
                stats_output = stats_grabber(route_batch)

                if stats_output is None:
                    logging.error(f'Got none type processing batch {int(i / BATCH_SIZE)}')
                    continue

                for key, value in stats_output.items():
                    if key == 'count':
                        value.to_sql(name='stats_count', con=conn.engine, if_exists='append', index=True,
                                     index_label='page_id')
                    else:
                        value.to_sql(name=f'stats_{key}', con=conn.engine, if_exists='append', index=False)

                # Add processing time to batch_time and estimate time remaining
                batch_times.append((time.time() - start_time) / len(stats_output['count']))
                logging.info(f'Data was just inserted for batch {int(i / BATCH_SIZE)}')
                logging.info(f'Total time for batch {time.time() - start_time}')
                logging.info(f'Projected time remaining is {(mean(batch_times) * len(filtered_list[i:]) / 3600)} hours')
                print(
                    f'Batch #{int(i / BATCH_SIZE)} - {round((mean(batch_times) * len(filtered_list[i:]) / 3600), 2)} hours remaining')

    except Exception as e:
        logging.error(f'An error occurred in the main function: {e}')
        print(f'An error occurred in the main function processing batch: {e}')

    finally:
        logging.info(f'Finished running at {datetime.datetime.now()}')


### EXECUTION ###
if __name__ == '__main__':
    main()
