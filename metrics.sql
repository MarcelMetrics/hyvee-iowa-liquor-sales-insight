USE int_hyvee;

ALTER TABLE transactions
    ADD COLUMN bottle_profit DECIMAL(10, 2),
	ADD COLUMN gallon_cost DECIMAL(10, 2),
    ADD COLUMN gallon_price DECIMAL(10, 2),
	ADD COLUMN sale_gallons DECIMAL(10, 2),   
    ADD COLUMN gallon_profit DECIMAL(10, 2),
    ADD COLUMN liter_cost DECIMAL(10, 2),
    ADD COLUMN liter_price DECIMAL(10, 2),
	ADD COLUMN sale_liters DECIMAL(10, 2),
    ADD COLUMN liter_profit DECIMAL(10, 2),
	ADD COLUMN cost DECIMAL(10, 2),
    ADD COLUMN revenue DECIMAL(10, 2),
    ADD COLUMN profit DECIMAL(10, 2),
    ADD COLUMN margin DECIMAL(10, 4);

BEGIN;

UPDATE transactions
SET bottle_profit = ROUND(bottle_price - bottle_cost, 2),
    revenue = ROUND(bottle_price * sale_bottles, 2),
    profit = ROUND((bottle_price - bottle_cost) * sale_bottles, 2),
    margin = ROUND(profit / revenue, 4),
    cost = ROUND(bottle_cost * sale_bottles, 2),
	sale_gallons = ROUND((bottle_volume_ml * sale_bottles) / 3785.411784, 2),
    gallon_cost = ROUND(cost / sale_gallons, 2),
    gallon_price = ROUND(revenue / sale_gallons, 2),
    gallon_profit = ROUND(profit / sale_gallons, 2),
	sale_liters = ROUND((bottle_volume_ml * sale_bottles) / 1000, 2),
    liter_cost = ROUND(cost / sale_liters, 2),
    liter_price = ROUND(revenue / sale_liters, 2),
    liter_profit = ROUND(profit / sale_liters, 2);

COMMIT;
