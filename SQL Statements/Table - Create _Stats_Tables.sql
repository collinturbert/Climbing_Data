CREATE TABLE IF NOT EXISTS stats_ticks (
	table_count INT AUTO_INCREMENT PRIMARY KEY,
    page_id INT,
	user VARCHAR(255),
    date DATE,
    style VARCHAR(255),
    leadStyle VARCHAR(255),
    pitches INT,
    text TEXT,
    comment TEXT,
    createdAt DATE,
    updatedAt DATE);
              

CREATE TABLE IF NOT EXISTS stats_ratings (
	table_count INT AUTO_INCREMENT PRIMARY KEY,
	page_id INT,
	user VARCHAR(255),
	allRatings VARCHAR(255),
    rockRating VARCHAR(255),
    iceRating VARCHAR(255),
    aidRating VARCHAR(255),
    boulderRating VARCHAR(255),
    mixedRating VARCHAR(255),
    snowRating VARCHAR(255),
    safteyRating TEXT,
	createdAt DATE,
	updatedAt DATE);


CREATE TABLE IF NOT EXISTS stats_stars (
	table_count INT AUTO_INCREMENT PRIMARY KEY,
	page_id INT,
	user VARCHAR(255),
	score VARCHAR(255),
	createdAt DATE,
	updatedAt DATE);
                
CREATE TABLE IF NOT EXISTS stats_todos (
	table_count INT AUTO_INCREMENT PRIMARY KEY,
	page_id INT,
	user VARCHAR(255),
	createdAt DATE,
	updatedAt DATE);

     
CREATE TABLE IF NOT EXISTS stats_count (
	page_id INT PRIMARY KEY,
	date_added DATE,
	stars INT,
	ratings INT,
	todos INT,
    ticks INT);