-- 1. Database Creation

CREATE TABLE sales_sample (
    product_id INTEGER,
    region VARCHAR(50),
    date DATE,
    sales_amount NUMERIC
);

-- 2. Data Creation

INSERT INTO sales_sample (product_id, region, date, sales_amount) 
VALUES 
    (101, 'Uttar Pradesh', '2025-01-15', 5000),
    (102, 'Maharashtra', '2025-01-20', 6500),
    (103, 'Tamil Nadu', '2025-02-01', 4200),
    (101, 'West Bengal', '2025-02-15', 3800),
    (104, 'Punjab', '2025-03-01', 7200),
    (102, 'Gujarat', '2025-03-15', 5900),
    (105, 'Karnataka', '2025-03-20', 4800),
    (103, 'Kerala', '2025-04-01', 6100),
    (104, 'Rajasthan', '2025-04-15', 5500),
    (105, 'Madhya Pradesh', '2025-04-30', 4900);

select * from sales_sample;
/* 3. Perform OLAP operations
 a) Drill Down-Analyze sales data at a more detailed level. Write a query to perform drill down from region to product level to understand sales performance.*/

SELECT 
    region,
    product_id,
    COUNT(*) as number_of_transactions,
    SUM(sales_amount) as total_sales,
    ROUND(AVG(sales_amount), 2) as avg_sale_per_product
FROM sales_sample
GROUP BY region, product_id
ORDER BY region, total_sales DESC;

-- b) Rollup- To summarize sales data at different levels of granularity. Write a query to perform roll up from product to region level to view total sales by region.

WITH sales_rollup AS (
    SELECT 
        COALESCE(region, 'Grand Total') as region,
        COALESCE(CAST(product_id AS VARCHAR), 'Region Total') as product_id,
        SUM(sales_amount) as total_sales,
        SUM(SUM(sales_amount)) OVER() as grand_total
    FROM sales_sample
    GROUP BY ROLLUP(region, product_id)
)
SELECT 
    region,
    product_id,
    total_sales,
    ROUND((total_sales / grand_total * 100), 2) as percentage_of_total
FROM sales_rollup
ORDER BY 
    CASE WHEN region = 'Grand Total' THEN 2
         WHEN product_id = 'Region Total' THEN 1
         ELSE 0 END,
    region,
    product_id;


-- c) Cube - To analyze sales data from multiple dimensions simultaneously. Write a query to Explore sales data from different perspectives, such as product, region, and date.


WITH sales_cube AS (
    SELECT 
        COALESCE(region, 'All Regions') as region,
        COALESCE(CAST(product_id AS VARCHAR), 'All Products') as product_id,
        COUNT(*) as transactions,
        SUM(sales_amount) as total_sales,
        ROUND(AVG(sales_amount), 2) as avg_sale
    FROM sales_sample
    GROUP BY CUBE(region, product_id)
)
SELECT 
    region,
    product_id,
    transactions,
    total_sales,
    avg_sale,
    ROUND((total_sales * 100.0 / 
        (SELECT total_sales FROM sales_cube 
         WHERE region = 'All Regions' AND product_id = 'All Products')), 2) as pct_of_total
FROM sales_cube
ORDER BY 
    CASE 
        WHEN region = 'All Regions' AND product_id = 'All Products' THEN 1  -- Grand Total
        WHEN region = 'All Regions' THEN 2  -- Product Totals
        WHEN product_id = 'All Products' THEN 3  -- Region Totals
        ELSE 4  -- Individual combinations
    END,
    region,
    product_id;



-- d) Slice- To extract a subset of data based on specific criteria. Write a query to slice the data to view sales for a particular region or date range.


WITH regional_slice AS (
    SELECT 
        product_id,
        date,
        sales_amount,
        SUM(sales_amount) OVER () as region_total
    FROM sales_sample
    WHERE region = 'Maharashtra'  -- Change region as needed
)
SELECT 
    product_id,
    TO_CHAR(date, 'YYYY-MM-DD') as sale_date,
    sales_amount,
    ROUND((sales_amount * 100.0 / region_total), 2) as pct_of_region_sales
FROM regional_slice
ORDER BY date, product_id;

-- e) Dice - To extract data based on multiple criteria. Write a query to view sales for specific combinations of product, region, and date

WITH dice_analysis AS (
    SELECT 
        region,
        product_id,
        date,
        sales_amount,
        AVG(sales_amount) OVER () as overall_avg,
        SUM(sales_amount) OVER () as filtered_total
    FROM sales_sample
    WHERE 
        region IN ('Maharashtra', 'Gujarat')  -- Specific regions
        AND product_id IN (101, 102)         -- Specific products
        AND date BETWEEN '2025-01-01' AND '2025-03-31'  -- Specific time period
)
SELECT 
    region,
    product_id,
    TO_CHAR(date, 'YYYY-MM-DD') as sale_date,
    sales_amount,
    ROUND((sales_amount - overall_avg), 2) as diff_from_avg,
    ROUND((sales_amount * 100.0 / filtered_total), 2) as pct_of_total,
    RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) as rank_in_region
FROM dice_analysis
ORDER BY 
    region,
    date,
    product_id;




