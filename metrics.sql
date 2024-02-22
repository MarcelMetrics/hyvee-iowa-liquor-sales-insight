USE int_hyvee;

-- Total volume of liquor ordered in gallons
ALTER TABLE transactions
ADD COLUMN sale_gallons DECIMAL(10, 2);

UPDATE transactions
SET sale_gallons = ROUND((bottle_volume_ml * sale_bottles) / 3785.411784, 2);

-- Total volume of liquor ordered in liters
ALTER TABLE transactions
ADD COLUMN sale_liters DECIMAL(10, 2);

UPDATE transactions
SET sale_liters = ROUND((bottle_volume_ml * sale_bottles) / 1000, 2);

-- Gross profit per bottle
ALTER TABLE transactions
ADD COLUMN bottle_profit DECIMAL(10, 2);

UPDATE transactions
SET bottle_profit = ROUND(bottle_price - bottle_cost, 2);

-- Sales revenue
ALTER TABLE transactions
ADD COLUMN revenue DECIMAL(10, 2);

UPDATE transactions
SET revenue = ROUND(bottle_price * sale_bottles, 2);

-- Total Gross Profit
ALTER TABLE transactions
ADD COLUMN profit DECIMAL(10, 2);

UPDATE transactions
SET profit = ROUND((bottle_price - bottle_cost) * sale_bottles, 2);

-- Profit margin
ALTER TABLE transactions
ADD COLUMN margin DECIMAL(10, 2);

UPDATE transactions
SET margin = ROUND(profit / revenue, 2);

-- Gross cost
ALTER TABLE transactions
ADD COLUMN cost DECIMAL(10, 2);

UPDATE transactions
SET cost = ROUND(bottle_cost * sale_bottles, 2);

-- Cost per gallon
ALTER TABLE transactions
ADD COLUMN gallon_cost DECIMAL(10, 2);

UPDATE transactions
SET gallon_cost = ROUND(cost / sale_gallons, 2);

-- Price per gallon
ALTER TABLE transactions
ADD COLUMN gallon_price DECIMAL(10, 2);

UPDATE transactions
SET gallon_price = ROUND(revenue / sale_gallons, 2);

-- Profit per gallon
ALTER TABLE transactions
ADD COLUMN gallon_profit DECIMAL(10, 2);

UPDATE transactions
SET gallon_profit = ROUND(profit / sale_gallons, 2);

-- Cost per liter
ALTER TABLE transactions
ADD COLUMN liter_cost DECIMAL(10, 2);

UPDATE transactions
SET liter_cost = ROUND(cost / sale_liters, 2);

-- Price per liter
ALTER TABLE transactions
ADD COLUMN liter_price DECIMAL(10, 2);

UPDATE transactions
SET liter_price = ROUND(revenue / sale_liters, 2);

-- Profit per liter
ALTER TABLE transactions
ADD COLUMN liter_profit DECIMAL(10, 2);

UPDATE transactions
SET liter_profit = ROUND(profit / sale_liters, 2);