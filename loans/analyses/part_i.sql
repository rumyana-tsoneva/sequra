CREATE SCHEMA DB.SEQURA;
-- ========================= PART I - Data Extraction (SQL) ========================= --

-- We import the whole file in table RAW_DATA

CREATE OR REPLACE TABLE DB.SEQURA.FACT_ORDERS
AS SELECT 
        order_id::INT                       order_id
        ,shopper_id::INT                    shopper_id
        ,merchant_id::INT                   merchant_id
        ,TO_DATE(order_date, 'dd/mm/yy')    order_date
FROM DB.SEQURA.RAW_DATA
WHERE TRY_CAST(ORDER_ID AS INT) BETWEEN 1 AND 25
;

SELECT *
FROM DB.SEQURA.FACT_ORDERS
;


CREATE OR REPLACE TABLE DB.SEQURA.DIM_MERCHANTS
AS SELECT 
     order_id                merchant_id
     ,shopper_id             merchant_name
FROM DB.SEQURA.RAW_DATA
WHERE NOT TRY_CAST(ORDER_ID AS INT) BETWEEN 1 AND 25
;

-- Since we have sparse orders data - we don't have rows for every merchant and shopper each month we are going to need a date dimension.
-- Alternative approach would be to use another table in subquery where we know for sure we will have all dates (months) of interest.
CREATE OR REPLACE TABLE DB.SEQURA.DIM_DATE (
   MY_DATE          DATE        NOT NULL
  ,YEAR             SMALLINT    NOT NULL
  ,MONTH            DATE        NOT NULL
  ,MONTH_NAME       CHAR(3)     NOT NULL
  ,DAY_OF_MON       SMALLINT    NOT NULL
  ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
  ,WEEK_OF_YEAR     SMALLINT    NOT NULL
  ,DAY_OF_YEAR      SMALLINT    NOT NULL
)
AS
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))  -- Number of days after reference date in previous line
  )
  SELECT MY_DATE
        ,YEAR(MY_DATE)
        ,TO_DATE(LEFT(MY_DATE,7), 'yyyy-MM') 
        ,MONTHNAME(MY_DATE)
        ,DAY(MY_DATE)
        ,DAYOFWEEK(MY_DATE)
        ,WEEKOFYEAR(MY_DATE)
        ,DAYOFYEAR(MY_DATE)
    FROM CTE_MY_DATE
;

SELECT *
FROM DB.SEQURA.DIM_DATE
;


SELECT *
FROM DB.SEQURA.FACT_ORDERS
;

-- First, let's explore a table where we are going to have 1 row for every 12 months prior a purchase for all merchant-shopper combinations. This is going to be the intermediary step in the next example.
-- We keep the ORDER_ID only for months when the order actually happened.
SELECT 
    d.MONTH
    ,f.ORDER_DATE
    ,CASE
        WHEN d.MONTH = TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM') THEN f.ORDER_ID
        ELSE NULL
    END ORDER_ID
    -- ,f.ORDER_ID
    ,f.MERCHANT_ID
    ,f.SHOPPER_ID
FROM  DB.SEQURA.DIM_DATE d
JOIN DB.SEQURA.FACT_ORDERS f
-- We want to have 12 rows per order
    ON d.MONTH BETWEEN 
        TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM')  - INTERVAL '11 months' AND TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM') 
WHERE d.DAY_OF_MON = 1 -- Return 1 day per month instead of 28/29/30/31
ORDER BY f.MERCHANT_ID, f.ORDER_ID, f.ORDER_DATE, d.MONTH
;

-- In the following example:
-- EXPANDED_ORDERS: we add prior 11 months to all orders to flag whether there was an order or not for the given shopper-month
WITH 
    EXPANDED_ORDERS AS (
        SELECT 
            d.MONTH
            -- Flag rows where there was an order in the given month
            ,CASE
                WHEN d.MONTH = TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM') THEN 1
                ELSE 0
            END HAS_ORDER
            ,f.MERCHANT_ID
            ,f.SHOPPER_ID
        FROM  DB.SEQURA.DIM_DATE d
        JOIN DB.SEQURA.FACT_ORDERS f
            ON d.MONTH BETWEEN 
                TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM')  - INTERVAL '11 months' AND TO_DATE(LEFT(f.ORDER_DATE,7), 'yyyy-MM') 
        WHERE d.DAY_OF_MON = 1 
        GROUP BY ALL
        ORDER BY f.MERCHANT_ID, d.MONTH
    ),

-- Step where we mark the shopper as recurrent or not for given month
FLAG_SHOPPERS_AS_RECURRENT_PER_MONTH AS (

            SELECT 
            
                MONTH
                ,MERCHANT_ID
                ,SHOPPER_ID
                -- Has order in current month
                ,HAS_ORDER
                -- Has order in previous 11 months
                ,SUM(HAS_ORDER) OVER  (PARTITION BY MERCHANT_ID, SHOPPER_ID
                                      ORDER BY MONTH
                                      ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING)
                AS HAS_ORDER_PREVIOUS_11_MONTHS

                ,CASE
                    WHEN HAS_ORDER <> 0
                    AND 
                SUM(HAS_ORDER) OVER  (PARTITION BY MERCHANT_ID, SHOPPER_ID
                                      ORDER BY MONTH
                                      ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING) <> 0
                THEN 1 
                ELSE 0
                END AS IS_RECURRENT
            
            FROM EXPANDED_ORDERS
),

TOTAL_CUSTOMERS_PER_MONTH AS (
    SELECT 
        MERCHANT_ID
        ,TO_DATE(LEFT(ORDER_DATE,7), 'yyyy-MM') ORDER_MONTH
        ,COUNT(DISTINCT SHOPPER_ID) TOTAL_CUSTOMERS
    FROM DB.SEQURA.FACT_ORDERS
    GROUP BY 1,2
),

TOTAL_RECURRENT_PER_MONTH AS (
    SELECT 
        o.MONTH
        ,o.MERCHANT_ID
        ,m.MERCHANT_NAME
        ,SUM(IS_RECURRENT) NUMBER_RECURRENT
    FROM FLAG_SHOPPERS_AS_RECURRENT_PER_MONTH o
    
    LEFT JOIN DB.SEQURA.DIM_MERCHANTS m
       ON o.MERCHANT_ID = m.MERCHANT_ID    
    
    GROUP BY 1,2,3
)



SELECT 
    
    o.MERCHANT_ID
    ,o.MERCHANT_NAME
    ,o.MONTH
    ,o.NUMBER_RECURRENT
    ,COALESCE(t.TOTAL_CUSTOMERS, 0) TOTAL_CUSTOMERS
    ,COALESCE(ROUND(o.NUMBER_RECURRENT/NULLIF(COALESCE(t.TOTAL_CUSTOMERS, 0), 0) * 100, 2),0) AS RECURRENCE_RATE
FROM TOTAL_RECURRENT_PER_MONTH o

LEFT JOIN TOTAL_CUSTOMERS_PER_MONTH t
    ON t.MERCHANT_ID = o.MERCHANT_ID
    AND t.ORDER_MONTH = o.MONTH
    
-- Return rows only for months with actual orders
WHERE COALESCE(t.TOTAL_CUSTOMERS, 0) <> 0

ORDER BY MERCHANT_NAME, MONTH

;