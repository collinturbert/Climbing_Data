import os
import re
import time
import json
import logging
import requests
import mysql.connector
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from statistics import mean
from decouple import config
from concurrent.futures import ThreadPoolExecutor
from ratelimit import limits, sleep_and_retry

### CONFIGURATION ###

# Log Folder
LOG_FOLDER = 'log_files'
LOG_FILENAME = f'Logfile - Route Grabber ({datetime.date.today()}).log'

# Batch processing of URL's
BATCH_SIZE = 100  # also used to set max_workers

# Rate limiting
CALLS_PER_PERIOD = 40
TIME_PERIOD = 10  # seconds

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
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info(f'Log file created/accessed at {datetime.datetime.now()}')

    except Exception as e:
        print(f'Error creating log file: {e}')


# Create MySQL database connection
def connect_to_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info(f'Connected to database at {datetime.datetime.now()}')

        return conn

    except Exception as e:
        logging.error(f'Error establishing connection to database: {e}', exc_info=True)

    return None


# Create MySQL table
def create_table(cursor):
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS mp_route_info (
                page_id INT PRIMARY KEY UNIQUE,
                url VARCHAR(255) UNIQUE,
                last_update DATE,
                date_grabbed DATE,
                name VARCHAR(255),
                grade VARCHAR(255),
                long_grade VARCHAR(255),
                fa VARCHAR(255),
                route_type VARCHAR(255),
                long_route_type VARCHAR(255),
                distance_ft INT,
                pitches INT,
                fixed_pieces INT,
                stars FLOAT,
                votes INT,
                location VARCHAR(255),
                views INT,
                date_added DATE,
                shared_by VARCHAR(255),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                description TEXT,
                protection TEXT,
                directions TEXT,
                misc TEXT
            )
        """
        logging.info(f'Created/checked table in database at {datetime.datetime.now()}')
        cursor.execute(create_table_query)

    except Exception as e:
        logging.error(f'Error creating table in database: {e}', exc_info=True)


# Insert data into the database
def insert_data(cursor, route_info):
    try:
        # Create list of tuples with data to insert
        data_to_insert = [(data['Page ID'],
                           data['URL'],
                           data['Last Update'],
                           data['Date Grabbed'],
                           data['Name'],
                           data['Grade'],
                           data['Long Grade'],
                           data['FA'],
                           data['Route Type'],
                           data['Long Route Type'],
                           data['Distance'],
                           data['Pitches'],
                           data['Fixed Pieces'],
                           data['Stars'],
                           data['Votes'],
                           data['Location'],
                           data['Views'],
                           data['Date Added'],
                           data['Shared By'],
                           data['Latitude'],
                           data['Longitude'],
                           json.dumps(data['Description']),
                           json.dumps(data['Protection']),
                           json.dumps(data['Directions']), json.dumps(data['Misc']))
                          for data in route_info]

        # Query for inserting data, include update for when route exists
        insert_query = """
            INSERT INTO mp_route_info (page_id, url, last_update, date_grabbed, name,
                                      grade, long_grade, fa, route_type,
                                      long_route_type, distance_ft, pitches, fixed_pieces,
                                      stars, votes, location, views, date_added,
                                      shared_by, latitude, longitude, description,
                                      protection, directions, misc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                url = VALUES(url),
                last_update = VALUES(last_update),
                date_grabbed = VALUES(date_grabbed),
                name = VALUES(name),
                grade = VALUES(grade),
                long_grade = VALUES(long_grade),
                fa = VALUES(fa),
                route_type = VALUES(route_type),
                long_route_type = VALUES(long_route_type),
                distance_ft = VALUES(distance_ft),
                pitches = VALUES(pitches),
                fixed_pieces = VALUES(fixed_pieces),
                stars = VALUES(stars),
                votes = VALUES(votes),
                location = VALUES(location),
                views = VALUES(views),
                date_added = VALUES(date_added),
                shared_by = VALUES(shared_by),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                description = VALUES(description),
                protection = VALUES(protection),
                directions = VALUES(directions),
                misc = VALUES(misc)
            """
        cursor.executemany(insert_query, data_to_insert)
        logging.info('Successfully inserted data into database')

    except Exception as e:
        logging.error(f'Error inserting data into database: {e}', exc_info=True)


# Grab route urls to process from database (only processes route pages)
def get_urls(cursor):
    try:
        # Grab data
        urls_query = "SELECT * FROM mp_urls WHERE INSTR(url, '/route/')"
        cursor.execute(urls_query)
        rows = cursor.fetchall()

        # Get page_id, route_url, and last_update in separate lists
        page_id = [row[0] for row in rows]
        route_url = [row[2] for row in rows]
        last_update = [row[3] for row in rows]

        # Zip values into a list of tuples
        urls = list(zip(page_id, route_url, last_update))
        logging.info(f'Got list of route URLs at {datetime.datetime.now()}')

        return urls

    except Exception as e:
        logging.error(f'Error getting list of all urls from database: {e}', exc_info=True)


# Grab processed urls from database
def get_processed_data(cursor):
    try:
        # Get processed page_ids, urls, and when they were last updated
        get_query = 'SELECT page_id, url, last_update FROM mp_route_info'
        cursor.execute(get_query)
        rows = cursor.fetchall()

        # Get page_id, url, and last_update in separate lists
        processed_page_id = [row[0] for row in rows]
        processed_url = [row[1] for row in rows]
        processed_last_update = [row[2] for row in rows]

        # Zip values into a list of tuples
        processed_urls = list(zip(processed_page_id, processed_url, processed_last_update))
        logging.info(f'Got list of processed routes at {datetime.datetime.now()}')

        return processed_urls

    except Exception as e:
        logging.error(f'Error getting processed urls: {e}', exc_info=True)


# Get the list of titles and how they should be processed
def get_title_categories():
    try:
        # Get list of title categories from .csv
        file_name = 'title_counts.csv'
        df = pd.read_csv(file_name)

        # Create data frame
        crunched_df = df.groupby('Process As')['Title'].agg(list).reset_index()
        crunched_df.columns = ['Process As', 'Values']

        return crunched_df

    except Exception as e:
        logging.error(f'Error getting title categories: {e}', exc_info=True)


# Get information for each route with rate limiting
@sleep_and_retry
@limits(calls=CALLS_PER_PERIOD, period=TIME_PERIOD)
def route_grabber(page_id, url, last_update, retry=True):
    try:
        response = requests.get(url)
        last_update = pd.to_datetime(last_update)
        date_grabbed = time.strftime('%Y-%m-%d')

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            ##INDIVIDUAL ELEMENTS##
            # Route Name
            route_name = soup.find('h1').get_text(strip=True)

            # Route Grade
            grade_text = soup.find('h2', class_='inline-block mr-2').get_text(strip=True)
            grade = grade_text.split('YDS')[0]

            # Route Stars and Votes
            span_id = 'starsWithAvgText-' + str(page_id)
            rating = soup.find('span', id=span_id).get_text(strip=True)
            stars = float(rating.split()[1])
            votes = int(rating.split()[3].replace(',', ''))

            # Route Location
            location_div = soup.find('div', class_='mb-half small text-warm')
            location_links = location_div.find_all('a')
            locations = [link.get_text(strip=True) for link in location_links]
            location_tree = ' > '.join(locations).replace('All Locations > ', '')

            # Route Coordinates
            script_tag = soup.find('script', type='application/ld+json')
            json_content = json.loads(script_tag.contents[0])
            latitude = json_content['geo']['latitude']
            longitude = json_content['geo']['longitude']

            ##TITLE ELEMENTS##
            # Find all title elements and their corresponding text sections
            title_elements = soup.find_all('h2', class_='mt-2')
            text_elements = soup.find_all('div', class_='fr-view')

            # Get list of titles and which category they should go in
            title_categories_df = get_title_categories()

            # Process each title element and put (title, text) into corresponding list
            description = []
            directions = []
            protection = []
            misc = []

            for title, text in zip(title_elements, text_elements):
                title_text = title.get_text(strip=True)
                paragraph_text = re.sub(r'\s+', ' ', text.get_text(strip=True))

                if title_text in title_categories_df.iloc[0, 1]:
                    description.append(f'{title_text} - {paragraph_text}')
                elif title_text in title_categories_df.iloc[1, 1]:
                    directions.append(f'{title_text} - {paragraph_text}')
                elif title_text in title_categories_df.iloc[3, 1]:
                    protection.append(f'{title_text} - {paragraph_text}')
                else:
                    misc.append(f'{title_text} - {paragraph_text}')

            ##TABLE ELEMENTS##
            # FA
            fa_cell = soup.find('td', string='FA:')
            if fa_cell:
                fa_text = fa_cell.find_next('td').get_text(strip=True)
            else:
                logging.error(f"Couldn't find FA for {route_name}")

            # Route type, distance, and pitches
            type_cell = soup.find('td', string='Type:')
            if type_cell:
                long_route_type = type_cell.find_next('td').get_text(strip=True)
                type_text = (long_route_type.replace('\n', ' ').replace('  ', '').
                             replace('Fixed Hardware', ', Fixed Hardware:'))
                split_type = type_text.split(', ')
                distance = 0
                pitches = 1
                fixed_pieces = 0
                route_type = []
                for item in split_type:
                    if 'ft' in item:
                        distance = int(item.split(' ft ')[0])

                    elif 'pitches' in item:
                        pitches = int(item.replace(' pitches', ''))

                    elif 'Fixed Hardware:' in item:
                        fixed_pieces = int(item.replace('Fixed Hardware:', '').replace('(', '').replace(')', ''))

                    else:
                        route_type.append(item)

                route_type = ', '.join(route_type)

            else:
                logging.error(f"Couldn't find Route Type for {route_name}")

            # Page views
            views_cell = soup.find('td', string='Page Views:')
            if views_cell:
                views_text = views_cell.find_next('td').get_text(strip=True)
                views = int(views_text.split()[0].replace(',', ''))
            else:
                logging.error(f"Couldn't find Page Views for {route_name}")

            # Date Added and Shared By
            date_cell = soup.find('td', string='Shared By:')
            if date_cell:
                date_text = date_cell.find_next('td').get_text(strip=True).replace('·Updates', ' ')
                shared_value = re.split(r'(on\s[A-Za-z0-9]+\s(0?[1-9]|[12][0-9]|3[01]),\s[0-9]+)', date_text)
                shared_by = shared_value[0]
                date_value = re.search(r'([A-Z][a-z]{2} \d{1,2}, \d{4})', date_text)
                date_added = pd.to_datetime(date_value.group(0))
            else:
                logging.error(f"Couldn't find Date Added for {route_name}")

            ##CREATE OUTPUT##
            route_dictionary = {'Page ID': page_id,
                                'URL': url,
                                'Last Update': last_update,
                                'Date Grabbed': date_grabbed,
                                'Name': route_name,
                                'Grade': grade,
                                'Long Grade': grade_text,
                                'FA': fa_text,
                                'Route Type': route_type,
                                'Long Route Type': long_route_type,
                                'Distance': distance,
                                'Pitches': pitches,
                                'Fixed Pieces': fixed_pieces,
                                'Stars': stars,
                                'Votes': votes,
                                'Views': views,
                                'Location': location_tree,
                                'Date Added': date_added,
                                'Shared By': shared_by,
                                'Latitude': latitude,
                                'Longitude': longitude,
                                'Description': description,
                                'Protection': protection,
                                'Directions': directions,
                                'Misc': misc}

            return route_dictionary

        elif response.status_code == 429:
            logging.error(f'''Server timed out for url: {url}. The page returned a
                  {response.status_code} response code. This occurred using a calls
                  per period of {CALLS_PER_PERIOD} and a time period of {TIME_PERIOD}''')
            if retry:
                time.sleep(30)
                return route_grabber(page_id, url, retry=False)

            else:
                logging.error(f'''Paused for 100 seconds and tried again but didn't
                              work for url {url}''')
                return None

        elif response.status_code == 404:
            logging.error(f'''Issue processing url: {url}. The page returned a
                  {response.status_code} response code.''')
            print(f'''Issue processing url: {url}. The page returned a
                  {response.status_code} response code.''')

            return None

    except Exception as e:
        logging.error(f'Error processing URL {url}: {e}', exc_info=True)

    return None


# Main execution
def main():
    setup_logging()

    try:
        with connect_to_db() as conn:
            cursor = conn.cursor()
            # Create the table if it doesn't exist
            create_table(cursor)

            # Get a list of url's to process
            urls_list = get_urls(cursor)

            # Get a list of url's that have already been processed and take them out of the urls_list
            processed_urls_list = get_processed_data(cursor)
            filtered_list = list(set(urls_list).difference(processed_urls_list))
            logging.info(f'URLs list is {len(urls_list)} rows long')
            logging.info(f'Processed list is {len(processed_urls_list)} rows long')
            logging.info(f'Filtered list is {len(filtered_list)} rows long')

            # Track time it takes to process each batch
            batch_times = []

            # Process routes to get data in batches
            for i in range(0, len(filtered_list), BATCH_SIZE):
                start_time = time.time()
                route_batch = filtered_list[i:i + BATCH_SIZE]

                with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                    site_data = list(executor.map(lambda x: route_grabber(*x),
                                                  route_batch))
                route_info_list = [dictionary for dictionary in site_data if dictionary is not None]

                insert_data(cursor, route_info_list)
                conn.commit()

                batch_times.append((time.time() - start_time) / len(route_info_list))
                logging.info(f'Data was just inserted for batch {int(i / BATCH_SIZE)} of length {len(route_info_list)}')
                logging.info(f'Total time for batch {time.time() - start_time}')
                logging.info(f'Projected time remaining is {(mean(batch_times) * len(filtered_list[i:]) / 3600)} hours')
                print(f'Data was just inserted for batch: {int(i / BATCH_SIZE)}, length: {len(route_info_list)}')
                print(f'Projected time remaining is: {(mean(batch_times) * len(filtered_list[i:]) / 3600)} hours')

    except Exception as e:
        logging.error(f'Error occurred in main function:', exc_info=True)
        print(f'Error occurred in main function processing batch: {e}')

    finally:
        cursor.close()
        conn.close()


### Run ###
if __name__ == '__main__':
    main()
