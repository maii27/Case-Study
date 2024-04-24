CREATE OR REPLACE VIEW vw_liquor_transaction AS
SELECT
    invoice_number,
    date,
    store_number,
    store_name,
    address,
    zip_code,
    store_location,
    county_number,
    county,
    category,
    category_name,
    vendor_number,
    vendor_name,
    item_number,
    item_description,
    pack,
    bottle_volume_ml,
    state_bottle_cost,
    state_bottle_retail,
    bottles_sold,
    sales_amount,
    volume_sold_litres,
    volume_sold_gallons,
    product_category,
    update_timestamp
FROM
    liquor_transaction;
