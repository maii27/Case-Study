CREATE OR REPLACE VIEW vw_sales_transaction AS
SELECT
    id,
    code
    order_id,
    product,
    quantity_ordered,
    price_each,
    order_date,
    purchase_address,
    product_category,
    update_timestamp
FROM
    gld.sales_transaction