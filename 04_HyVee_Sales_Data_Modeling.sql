USE INT_HYVEE;

-- Create dimension tables

-- Locations
DROP TABLE IF EXISTS Locations;

CREATE TABLE Locations (
    zipcode VARCHAR(10) PRIMARY KEY,
    county VARCHAR(100),
    city VARCHAR(100)
);

INSERT INTO Locations (zipcode, county, city)
WITH RankedZipcodes AS (
    SELECT 
        zipcode, 
        county, 
        city,
        -- Select only the latest instance of any duplicated zipcode (possibly reassignment of zipcode)
        ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY date DESC) as rn
    FROM sales
)
SELECT 
    zipcode, 
    county, 
    city
FROM RankedZipcodes
WHERE rn = 1
ORDER BY zipcode;

-- Stores
DROP TABLE IF EXISTS Stores;

CREATE TABLE Stores (
    store_id INT PRIMARY KEY,
    store_format VARCHAR(100),
    store_name VARCHAR(255),
    zipcode VARCHAR(10),
    FOREIGN KEY (zipcode) REFERENCES Locations(zipcode)
);

INSERT INTO Stores (store_id, store_format, store_name, zipcode)
WITH RankedStores AS (
	SELECT 
		store,
		store_format, 
		name, 
		zipcode,	
        -- Select only the latest instance of any duplicated store id (possiblly callsign updated over time)
        ROW_NUMBER() OVER (PARTITION BY store ORDER BY date DESC) as rn
	FROM sales
)
SELECT DISTINCT
    store AS store_id,
    store_format, 
    name AS store_name, 
    zipcode
FROM RankedStores
WHERE rn = 1
ORDER BY store_id;

-- StoreFormats
DROP TABLE IF EXISTS StoreFormats;

CREATE TABLE StoreFormats (
    store_format_id INT PRIMARY KEY AUTO_INCREMENT,
    store_format VARCHAR(100) UNIQUE
);

INSERT INTO StoreFormats (store_format)
SELECT DISTINCT store_format 
FROM Stores
ORDER BY store_format;

ALTER TABLE Stores
ADD COLUMN store_format_id INT,
ADD FOREIGN KEY (store_format_id) REFERENCES StoreFormats(store_format_id);

UPDATE Stores s
INNER JOIN StoreFormats sf ON s.store_format = sf.store_format
SET s.store_format_id = sf.store_format_id;

ALTER TABLE Stores
DROP COLUMN store_format;

-- Categories
DROP TABLE IF EXISTS Categories;

CREATE TABLE Categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255)
);

INSERT INTO Categories(category_id, category_name)
SELECT DISTINCT
	category AS category_id,
    category_name
FROM sales
ORDER BY category;

-- Vendors
DROP TABLE IF EXISTS Vendors;

CREATE TABLE Vendors (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(255)
);

INSERT INTO Vendors(vendor_id, vendor_name)
WITH RankedVendors AS (
	SELECT 
		vendor_no,
		vendor_name,
		ROW_NUMBER() OVER(PARTITION BY vendor_no ORDER BY date DESC) AS rn
	FROM 
		sales
)
SELECT
	vendor_no AS vendor_id,
    vendor_name
FROM RankedVendors
WHERE rn = 1
ORDER BY vendor_no;

-- Products
SELECT DISTINCT 
	itemno AS product_id,
    im_desc AS product_info,
    category AS category_id,
    vendor_no AS vendor_id
FROM sales
ORDER BY itemno;
-- Too many duplicated product id's. Reassigning the id.

DROP TABLE IF EXISTS Products;

CREATE TABLE Products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
	product_info VARCHAR(255),
    category_id INT,
    vendor_id INT,
    FOREIGN KEY (category_id) REFERENCES Categories(category_id),
    FOREIGN KEY (vendor_id) REFERENCES Vendors(vendor_id)
);

INSERT INTO Products (product_info, category_id, vendor_id)
SELECT DISTINCT 
    im_desc AS product_info,
    category AS category_id,
    vendor_no AS vendor_id
FROM sales
ORDER BY product_info;

ALTER TABLE sales
ADD COLUMN product_id INT
;

-- Creating indexes for performance reasons
CREATE INDEX idx_sales_im_desc ON sales(im_desc);
CREATE INDEX idx_sales_category ON sales(category);
CREATE INDEX idx_sales_vendor_no ON sales(vendor_no);
CREATE INDEX idx_products_product_info ON Products(product_info);
CREATE INDEX idx_products_category_id ON Products(category_id);
CREATE INDEX idx_products_vendor_id ON Products(vendor_id);

SET SQL_SAFE_UPDATES = 0;
UPDATE sales s
INNER JOIN Products p 
    ON s.im_desc = p.product_info 
    AND s.category = p.category_id 
    AND s.vendor_no = p.vendor_id
SET s.product_id = p.product_id;
SET SQL_SAFE_UPDATES = 1;

-- Dropping indexes to save space
ALTER TABLE sales DROP INDEX idx_sales_im_desc;
ALTER TABLE sales DROP INDEX idx_sales_category;
ALTER TABLE sales DROP INDEX idx_sales_vendor_no;
ALTER TABLE Products DROP INDEX idx_products_product_info;
ALTER TABLE Products DROP INDEX idx_products_category_id;
ALTER TABLE Products DROP INDEX idx_products_vendor_id;

-- Create the fact table Transactions
DROP TABLE IF EXISTS Transactions;

CREATE TABLE Transactions (
    invoice_line_id BIGINT PRIMARY KEY,
    date DATE,
    store_id INT,
    product_id INT,
    cost DECIMAL(10, 2),
    price DECIMAL(10, 2),
    sales_volume INT,
    revenue DECIMAL(10, 2),
    FOREIGN KEY (store_id) REFERENCES Stores(store_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

INSERT INTO Transactions
SELECT 
	invoice_line_no AS invoice_line_id,
    date,
    store AS store_id,
    product_id,
    state_bottle_cost AS cost,
    state_bottle_retail as price,
    sale_bottles AS sales_volume,
    revenue
FROM sales
ORDER BY date, invoice_line_no;

DROP TABLE IF EXISTS sales;