
-- Sales Analysis
SELECT
    P.city AS City,
    P.product_line AS Product_line,
    C.customer_type AS Customer_type,
    SUM(f.sales) AS Total_sales,
    SUM(f.quantity) AS Total_quantity,
    AVG(f.sales) AS Avg_sale_value,

    DENSE_RANK() OVER (
        PARTITION BY p.city
        ORDER BY SUM(f.sales) DESC
    ) AS Sales_rank_in_city,

    SUM(SUM(f.sales)) OVER (
        PARTITION BY p.city
        ORDER BY SUM(f.sales) DESC
    ) AS cumulative_sales

FROM `project_id.sales_dw.fact_sales` f
JOIN `project_id.sales_dw.dim_product` p
    ON f.product_id = p.product_id
JOIN `project_id.sales_dw.dim_customer` c
    ON f.customer_id = c.customer_id

GROUP BY p.city, p.product_line, c.customer_type

ORDER BY City, Total_sales DESC

-- Top Customers Contribution

SELECT
    C.customer_type AS Customer_type,
    SUM(f.sales) AS Revenue,

    SUM(SUM(f.sales)) OVER(
        ORDER BY SUM(f.sales) DESC
    ) AS Cumulative_revenue,

    ROUND(
        100 * SUM(SUM(f.sales)) OVER(
            ORDER BY SUM(f.sales) DESC
        )
        / SUM(SUM(f.sales)) OVER(),
        2
    ) AS Cumulative_percentage

FROM `project_id.sales_dw.fact_sales` f
JOIN `project_id.sales_dw.dim_customer` c
    ON f.customer_id = c.customer_id

GROUP BY
    c.customer_type

ORDER BY
    Revenue DESC


--  Monthly Sales Trend Analysis

SELECT
p.city AS City,
FORMAT_DATE('%Y-%m', DATE(f.date)) AS Sales_month,
SUM(f.sales) AS Monthly_revenue,

IFNULL(LAG(SUM(f.sales)) OVER(
PARTITION BY p.city
ORDER BY FORMAT_DATE('%Y-%m', DATE(f.date))
),0) AS Previous_month_revenue,

IFNULL(SUM(f.sales)
- LAG(SUM(f.sales)) OVER(
PARTITION BY p.city
ORDER BY FORMAT_DATE('%Y-%m', DATE(f.date))
),0) AS Revenue_growth

FROM `project_id.sales_dw.fact_sales` f
JOIN `project_id.sales_dw.dim_product` p
ON f.product_id = p.product_id

GROUP BY
p.city,
sales_month

ORDER BY
City,
Sales_month
