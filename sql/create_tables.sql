CREATE OR REPLACE TABLE `project_id.sales_dw.dim_customer` (
    customer_id INT64,
    customer_type STRING,
    gender STRING,
    payment STRING
);

CREATE OR REPLACE TABLE `project_id.sales_dw.dim_product` (
    product_id INT64,
    branch STRING,
    city STRING,
    product_line STRING
);

CREATE OR REPLACE TABLE `project_id.sales_dw.fact_sales` (
    sale_id INT64,
    product_id INT64,
    customer_id INT64,
    date DATE,
    time TIME,
    quantity INT64,
    unit_price FLOAT64,
    tax FLOAT64,
    sales FLOAT64
)
PARTITION BY date
CLUSTER BY product_id, customer_id;
