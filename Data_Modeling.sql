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
    address VARCHAR(100),
    zipcode VARCHAR(10),
    city VARCHAR(100),
    county VARCHAR(100)
);

INSERT INTO Stores (store_id, store_name, address, zipcode, city, county)
WITH RankedStores AS (
	SELECT 
		store,
        name, 
		address, 
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
    address, 
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
    category AS category_code, 
    category_name AS category,
    vendor_name AS vendor
FROM sales
ORDER BY itemno;
-- Reassigning product IDs due to duplicates

DROP TABLE IF EXISTS Items;
CREATE TABLE Items (
    item_id INT PRIMARY KEY,
	item_name VARCHAR(255),
    category_code INT, 
    category VARCHAR(255),
    vendor VARCHAR(255)
);

INSERT INTO Items (item_id, item_name, category_code, category, vendor)
WITH RankedItems AS (
	SELECT 
		itemno,
        im_desc, 
        category, 
		category_name, 
		vendor_name,
        -- Select only the latest instance of any duplicated item id (possiblly callsign updated over time)
        ROW_NUMBER() OVER (PARTITION BY itemno ORDER BY date DESC) as rn
	FROM sales
)
SELECT DISTINCT 
	itemno AS item_id,
    im_desc AS item_name,
    category AS category_code,
    category_name AS category,
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
    bottle_volume_ml INT,
    bottle_cost DECIMAL(10, 2),
    bottle_price DECIMAL(10, 2),
    sale_bottles INT
);

INSERT INTO Transactions
SELECT 
	invoice_line_no AS invoice_line_id,
    date,
    store AS store_id,
    itemno AS item_id,
    bottle_volume_ml,
    state_bottle_cost AS bottle_cost,
    state_bottle_retail as bottle_price,
    sale_bottles
FROM sales
ORDER BY date, invoice_line_no;

ALTER TABLE Transactions ADD CONSTRAINT fk_transactions_store_id FOREIGN KEY (store_id) REFERENCES Stores(store_id);
ALTER TABLE Transactions ADD CONSTRAINT fk_transactions_item_id FOREIGN KEY (item_id) REFERENCES Items(Item_id);
ALTER TABLE Transactions ADD CONSTRAINT fk_transactions_date FOREIGN KEY (date) REFERENCES Calendar(date);

-- DROP TABLE IF EXISTS sales;