-- Environment Setup

START TRANSACTION;

DROP DATABASE IF EXISTS STG_HYVEE;
CREATE DATABASE STG_HYVEE;

DROP DATABASE IF EXISTS INT_HYVEE;
CREATE DATABASE INT_HYVEE;

COMMIT;

-- Staging Layer Setup

USE STG_HYVEE;

CREATE TABLE sales (
    invoice_line_no BIGINT,
    date DATE,
    store INT,
    name VARCHAR(255),
    city VARCHAR(100),
    zipcode INT,
    county VARCHAR(100),
    category INT,
    category_name VARCHAR(255),
    vendor_no INT,
    vendor_name VARCHAR(255),
    itemno INT,
    im_desc VARCHAR(255),
    state_bottle_cost DECIMAL(10, 2),
    state_bottle_retail DECIMAL(10, 2),
    sale_bottles INT,
    revenue DECIMAL(10, 2),
    store_format VARCHAR(255)
);

SET GLOBAL local_infile = 'ON';
SHOW VARIABLES LIKE 'local_infile';

LOAD DATA 
	LOCAL INFILE "D:/GitHub/hyvee-iowa-liquor-sales-insight/data/clean_hyvee.csv"
	INTO TABLE sales
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS;
    
SELECT count(*) FROM sales;

-- Integration Layer Setup

USE INT_HYVEE;

CREATE TABLE INT_HYVEE.sales LIKE STG_HYVEE.sales;

LOAD DATA 
	LOCAL INFILE "D:/GitHub/hyvee-iowa-liquor-sales-insight/data/clean_hyvee.csv"
	INTO TABLE sales
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS;

SELECT count(*) FROM sales;

SET GLOBAL local_infile = 'OFF';