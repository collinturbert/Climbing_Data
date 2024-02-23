import os
import re
import logging
import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import date
from decouple import config

### CONFIGURATION ###

# If running as complete update set to True, if continuing interrupted execution set to False
UPDATE = True

# Processed in batches, also used to set max_workers
BATCH_SIZE = 40


### FUNCTIONS ###

# Initialize the logging system
def setup_logging():
    log_folder = 'log_files'
    os.makedirs(log_folder, exist_ok=True)

    log_filename = f'Logfile - URL Grabber ({date.today()}).log'
    log_file = os.path.join(log_folder, log_filename)
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


# Create MySQL database connection
def connect_to_db():
    try:
        db_config = dict(host=config('AWS_HOST'), user=config('AWS_USERNAME'), password=config('AWS_MASTER_PASSWORD'),
                         database=config('AWS_DATABASE'))

        conn = mysql.connector.connect(**db_config)

        return conn

    except Exception as e:
        logging.error(f'Error connecting to database: {e}')

    return None


# Create MySQL table if it doesn't exist
def create_table(cursor, update):
    try:
        # If completely updating the url's table, delete it first
        if update:
            drop_query = "DROP TABLE IF EXISTS mp_urls"
            cursor.execute(drop_query)

        # Create new version of table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS mp_urls (
                page_id INT PRIMARY KEY UNIQUE,
                sitemap VARCHAR(255),
                url VARCHAR(255) UNIQUE,
                last_update DATE
            )
        """
        cursor.execute(create_table_query)

    except Exception as e:
        logging.error(f'Error creating/dropping urls table: {e}')

    return None


# Grab processed sitemaps from database
def get_data(cursor, update):
    try:
        # If running a complete update drop the table and start with a blank list
        if update:
            processed_sitemaps = []

        # If continuing process grab which sitemaps have been processed already
        else:
            get_query = "SELECT DISTINCT sitemap FROM mp_urls"
            cursor.execute(get_query)
            processed_sitemaps = [row[0] for row in cursor.fetchall()]

        return processed_sitemaps

    except Exception as e:
        logging.error(f'Error getting sitemaps from database: {e}')

    return None


# Insert data into the database
def insert_data(cursor, url_list):
    try:
        data_to_insert = [(data['Page ID'], data['Sitemap'], data['URL'],
                           data['Last Update']) for data in url_list]

        insert_query = """
            INSERT INTO mp_urls (page_id, sitemap, url, last_update)
            VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data_to_insert)

    except Exception as e:
        logging.error(f'Error inserting data in database: {e}')

    return None


# Function for processing the sitemap and finding each sub-sitemap
def get_sitemaps(sitemap_url):
    try:
        response = requests.get(sitemap_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'xml')
            loc_tags = soup.find_all('loc')
            sitemap_list = [loc_tag.text for loc_tag in loc_tags]
            return sitemap_list

        else:
            logging.error("Failed to retrieve sitemap url's.")

    except Exception as e:
        logging.error(f'Error processing sitemap for URL {sitemap_url}: {e}')

    return None


# Get URL information from each sitemap XML
def get_urls(url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'xml')
            url_tags = soup.find_all('url')
            site_data = []

            for url_tag in url_tags:
                loc_tag = url_tag.find('loc')
                lastmod_tag = url_tag.find('lastmod')
                page_id_match = re.search(r'[0-9]+', loc_tag.text)

                if page_id_match:
                    page_id = page_id_match.group(0)
                else:
                    page_id = 000000
                    logging.error(f"couldn't find route ID for {url}")

                # Create Output
                url_dictionary = {'Page ID': page_id, 'Sitemap': url,
                                  'URL': loc_tag.text,
                                  'Last Update': lastmod_tag.text}
                site_data.append(url_dictionary)
            return site_data

    except Exception as e:
        logging.error(f'Error processing URL {url}: {e}')

    return None


# Main execution
def main():
    setup_logging()

    try:
        with connect_to_db() as conn:
            # Create cursor object
            cursor = conn.cursor(buffered=True)

            # Make sure table exists in database, drop if refreshing
            create_table(cursor, UPDATE)

            # Get urls from sitemap and filter already processed urls
            sitemap_url = config('MP_SITEMAP')
            sitemap_list = get_sitemaps(sitemap_url)[1:]  # Dropped 'pages' sitemap
            processed_sitemaps = get_data(cursor, UPDATE)
            filtered_list = list(set(sitemap_list).difference(processed_sitemaps))

            # Process sitemaps in batches
            for i in range(0, len(filtered_list), BATCH_SIZE):
                sitemap_batch = filtered_list[i:i + BATCH_SIZE]

                with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                    site_data = list(executor.map(get_urls, sitemap_batch))

                if len(site_data) != 0:
                    url_list = [dictionary for data_list in site_data
                                for dictionary in data_list]
                    insert_data(cursor, url_list)

                    conn.commit()
                else:
                    logging.error(f'Got a blank site_data list for batch {i / BATCH_SIZE}')

    except Exception as e:
        logging.error(f'Error occurred in main function: {e}')

    finally:
        cursor.close()
        conn.close()


### Run ###

if __name__ == '__main__':
    main()
