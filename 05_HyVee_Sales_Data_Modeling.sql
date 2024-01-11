/* 
================================
DATA MODELING
================================
*/

USE INT_HYVEE;

/*
--------------------
DIMENSION TABLE 1: Stores
--------------------
Contains unique stores and their info.
*/

DROP TABLE IF EXISTS Stores;

CREATE TABLE Stores (
    store_id INT PRIMARY KEY,
	store_name VARCHAR(255),
    store_format VARCHAR(100),
    zipcode VARCHAR(10),
    city VARCHAR(100),
    county VARCHAR(100)
);

INSERT INTO Stores (store_id, store_name, store_format, zipcode, city, county)
WITH RankedStores AS (
	SELECT 
		store,
        name, 
		store_format, 
		zipcode,
		city,
		county,
        -- Select only the latest instance of any duplicated store id (possiblly callsign updated over time)
        ROW_NUMBER() OVER (PARTITION BY store ORDER BY date DESC) as rn
	FROM sales
)
SELECT DISTINCT
    store AS store_id,
    name AS store_name, 
    store_format, 
    zipcode,
    city,
    county
FROM RankedStores
WHERE rn = 1
ORDER BY store_id;

/*
--------------------
DIMENSION TABLE 2: Items
--------------------
Detailed information about each item.
*/

SELECT DISTINCT 
	itemno AS item_id,
    im_desc AS item_name,
    category_name AS subcategory,
    liquor_type AS category,
    vendor_name AS vendor

FROM sales
ORDER BY itemno;
-- Reassigning product IDs due to duplicates

DROP TABLE IF EXISTS Items;

CREATE TABLE Items (
    item_id INT PRIMARY KEY,
	item_name VARCHAR(255),
    subcategory VARCHAR(255),
    category VARCHAR(255),
    vendor VARCHAR(255)
);

INSERT INTO Items (item_id, item_name, subcategory, category, vendor)
WITH RankedItems AS (
	SELECT 
		itemno,
        im_desc, 
		category_name, 
		liquor_type,
		vendor_name,
        -- Select only the latest instance of any duplicated item id (possiblly callsign updated over time)
        ROW_NUMBER() OVER (PARTITION BY itemno ORDER BY date DESC) as rn
	FROM sales
)
SELECT DISTINCT 
	itemno AS item_id,
    im_desc AS item_name,
    category_name AS subcategory,
    liquor_type AS category,
    vendor_name AS vendor
FROM RankedItems
WHERE rn = 1
ORDER BY item_id;

/*
--------------------
FACT TABLE: Transactions
--------------------
Capturing each sales transaction in detail.
*/

DROP TABLE IF EXISTS Transactions;

CREATE TABLE Transactions (
    invoice_line_id VARCHAR(255) PRIMARY KEY,
    date DATE,
    store_id INT,
    item_id INT,
    cost DECIMAL(10, 2),
    price DECIMAL(10, 2),
    sales_volume INT,
    FOREIGN KEY (store_id) REFERENCES Stores(store_id),
    FOREIGN KEY (item_id) REFERENCES Items(Item_id),
    FOREIGN KEY (date) REFERENCES Calendar(date)
);

INSERT INTO Transactions
SELECT 
	invoice_line_no AS invoice_line_id,
    date,
    store AS store_id,
    itemno AS item_id,
    state_bottle_cost AS cost,
    state_bottle_retail as price,
    sale_bottles AS sales_volume
FROM sales
ORDER BY date, invoice_line_no;

DROP TABLE IF EXISTS sales;