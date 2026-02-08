-------------------------------Data Cleaning--------------------------------

-------------------------- In Order[Fact Table]---------------

------Order out of the time period given----


select * from (select * from Orders 
where Bill_date_timestamp not between  '2021-09-01'  and  '2023-12-31') o
left join (select order_id, sum(payment_value) as payment_values from OrderPayments
group by order_id ) as op  
on o.order_id=op.order_id

----------------------Solution------------------
select * into order_clean from Orders
delete from order_clean 
where Bill_date_timestamp 
not between  '2021-09-01' and 
'2023-12-31'


----------certain cummlative quantity count -----
select * from order_clean  

--------------check certain cummlative quantity count------

with quantity_rank AS (select *, 
row_number() over(partition by customer_id,order_id,product_id order by Quantity desc)
as rn from order_clean )
select * from quantity_rank
where  rn>1

---------------------solution ----------------------
--------------keep max_qty and remove less than max_qty---------------
with t as ( select * , max(Quantity) over (partition by customer_id, order_id ,product_id) as max_qty from order_clean
)
delete  from t 
where  Quantity < max_qty 

select count(*) as No_of_rows from order_clean 


--------------------One order mapped with multiple stores where instore channel-------------
select * into order_cleanc from order_clean

 select order_id ,count(distinct Delivered_StoreID) as store_count  from order_clean
 where Channel='Instore'
 Group by order_id 
 having count(distinct Delivered_StoreID)>1

 SELECT  *, ROW_NUMBER() over (partition by order_id order by Total_Amount desc) as ranks
FROM order_clean
WHERE Channel = 'Instore'
AND order_id IN (
 select order_id  from order_clean
 where Channel='Instore'
 Group by order_id 
 having count(distinct Delivered_StoreID)>1
 )

---------------------solution-------------------

WITH rankedstores as ( SELECT  *, ROW_NUMBER() over (partition by order_id order by Total_Amount desc) as ranks
FROM order_cleanc
WHERE Channel = 'Instore'
AND order_id IN (
 select order_id  from order_cleanc
 where Channel='Instore'
 Group by order_id 
 having count(distinct Delivered_StoreID)>1
 ))
update oc 
set oc.Delivered_StoreID=rs1.Delivered_StoreID from order_cleanc oc
join rankedstores rs
on oc.order_id= rs.order_id 
join rankedstores rs1
on oc.order_id= rs1.order_id
and rs1.ranks=1
where rs.ranks>1
and oc.order_id=rs.order_id



--------------------------------one order mapped with multiple  bil_time_stamp ----------------
 select * into order_cleanco from order_cleanc

Select * from order_cleanc
where order_id in (SELECT order_id
FROM order_cleanc
GROUP BY order_id
HAVING COUNT( distinct Bill_date_timestamp) > 1)

------------------------------------solution-------------------------------------

WITH FirstBill AS (
    SELECT
        order_id,
        MIN(Bill_date_timestamp) AS first_bill_date
    FROM order_cleanco
    GROUP BY order_id
)
UPDATE o
SET o.Bill_date_timestamp = f.first_bill_date
FROM order_cleanco o
JOIN FirstBill f
  ON o.order_id = f.order_id
WHERE o.Bill_date_timestamp <> f.first_bill_date


select * from order_cleanco
where order_id in (SELECT order_id
FROM order_cleanco
GROUP BY order_id
HAVING COUNT( distinct Customer_id) > 1)

--------------------------solution-----------------------
select * into order_cleanf from order_cleanco
WITH cust_choice AS (
    SELECT 
        order_id,
		Customer_id AS chosen_custid
    FROM (
        SELECT Customer_id,
            order_id,
            Total_Amount,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY Total_Amount DESC, Customer_id ASC
            ) AS rn
        FROM order_cleanf
    ) t
    WHERE rn = 1
)
UPDATE o
SET o.Customer_id = c.chosen_custid
FROM order_cleanf o
JOIN cust_choice c
  ON o.order_id = c.order_id
WHERE o.Customer_id <> c.chosen_custid  -- only update rows that actually need change

select * from order_cleanf
where order_id='005d9a5423d47281ac463a968b3936fb'

with t as ( select * , max(Quantity) over (partition by customer_id, order_id ,product_id) as max_qty from order_clean
)
delete  from t 
where  Quantity < max_qty 

------------check mismatch value------------
 select  * from (select order_id , sum(Total_Amount) as Total_Amount from order_cleanf
group by order_id ) as oc
inner join (select order_id, sum(payment_value) as payment_values from OrderPayments
group by order_id ) as op 
on oc.order_id=op.order_id
where oc.Total_Amount<>op.payment_values



 select  * from (select order_id , round(sum(Total_Amount),0) as Total_Amount from order_cleanf
group by  order_id ) as oc
inner join (select order_id, round(sum(payment_value),0) as payment_value from OrderPayments
group by order_id ) as op 
on oc.order_id=op.order_id
where oc.Total_Amount<>op.payment_value

select * from order_cleanf
---------------------in  order table which order is not present in order_payment ------------- 
select * FROM order_cleanf
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM OrderPayments)

----------------in OrderPayments table -------------------
----------------where payment_value is zero-----------
select * from OrderPayments
where payment_value=0

delete from OrderPayments
where payment_value=0

-------------------------create a new table on basis of payment_types in orderPayment---------------

select * into order_pay from OrderPayments

---------------------------solution ------------------
SELECT 
    order_id,
    ISNULL([voucher], 0) AS Voucher,
    ISNULL([UPI/Cash], 0) AS UPI_Cash,
    ISNULL([credit_card], 0) AS Credit_Card,
    ISNULL([debit_card], 0) AS Debit_Card,
    COALESCE([voucher],0) + COALESCE([UPI/Cash],0) + 
    COALESCE([credit_card],0) + COALESCE([debit_card],0) AS Total_Amount
into order_payment FROM (
    SELECT * 
    FROM order_pay
) AS src
PIVOT (
    SUM(payment_value)
    FOR payment_type IN ([voucher], [UPI/Cash], [credit_card], [debit_card])
) AS pvt
ORDER BY order_id 




select  * from (select order_id ,round(sum(Total_Amount),0) as Total_Amount from order_cleanf
group by  order_id ) as oc
inner join (select order_id ,round(sum(Total_Amount),0) as Total_Amount from order_payment
group by order_id ) as op 
on oc.order_id=op.order_id
where oc.Total_Amount<>op.Total_Amount

-----------------------Data Cleaning in Product_Info Table---------------

----- update the incorrect spelt column name to correct column name 
----Before Cleaning
select * from ProductsInfo 

--After Cleaning--

Exec sp_rename
'ProductsInfo.product_name_lenght','product_name_length','Column'

Exec sp_rename
'ProductsInfo.product_description_lenght','product_description_length','Column'

select * 
from ProductsInfo

--In product_info table There are 623 row are ‘#N/A’ and Null value. So here Replace the ‘#N/A’ to ‘other'
--and Replace the Null value to ‘0’--

--Before Cleaning

select *
from ProductsInfo
where Category is null or Category = '#N/A'

--After Cleaning

update ProductsInfo
set 
   Category = Case 
              When Category IS Null OR Category = '#N/A' then 'other'
			  Else Category
			  End,
   product_name_length = isnull(product_name_length,0),
   product_description_length = isnull(product_description_length,0),
   product_photos_qty = isnull(product_photos_qty,0),
   product_weight_g = isnull(product_weight_g,0),
   product_length_cm = isnull(product_length_cm,0),
   product_height_cm = isnull(product_height_cm,0),
   product_width_cm = isnull(product_width_cm,0)


select *
from ProductsInfo
where Category = 'other'



------------------------------------------Data cleaning in stores_info Table--------------------------------------

----In stores_info table the StoreID ST410 have duplicates value

---Before cleaning

select *
from stores_info
where StoreID in (
select StoreID
from stores_info
group by StoreID
having count(*)>1
)
order by StoreID

--After Cleaning

with duplicates as (
    select *,
	       ROW_NUMBER() over(partition by StoreID order by storeID) as row_num
	from stores_info
)
Delete from duplicates
where row_num>1


select *
from stores_info
where StoreID = 'ST410'




--------------------------------Data clening  in order_review_rating table----------------------------------



-----In order_review_rating table there are 100,000 rows but 99,441 are unique order and single review rating but 559
-----Have  multiple review rating


--Before cleaning

--Here 100,000 Rows

select *
from OrderReview_Ratings

--After Cleaning

----Here resolve the multiple review rating to calculate their average review rating and modify new table and table name is order_review_ratings  and  99,441 rows are affected 


Exec sp_help OrderReview_Ratings

select * from (select order_id,
Avg(cast(Customer_Satisfaction_Score as float)) as Customer_sat_avg_rating 
from OrderReview_Ratings
group by order_id) as c


select order_id,
Avg(cast(Customer_Satisfaction_Score as float)) as cust_sat_avg_rating into orderreviews_ratings
from OrderReview_Ratings
group by order_id

select * from orderreviews_ratings


-------------------------Create Customer 360 Table -------------------

/* =========================
   CUSTOMER 360 (SQL Server)
   ========================= */

WITH oc AS (
    SELECT
        o.Customer_id,
        o.order_id,
        o.product_id,
        o.Channel,
        o.Delivered_StoreID,
        CAST(o.Bill_date_timestamp AS datetime) AS bill_ts,
        o.Quantity,
        o.Cost_Per_Unit,
        o.MRP,
        o.Discount,
        o.Total_Amount,
        /* Profit model — adjust if your business rule differs */
        ((o.MRP - o.Cost_Per_Unit) * o.Quantity) - ISNULL(o.Discount,0) AS profit
    FROM order_cleanf AS o
),
-- Join product/stores for category & city/state
oc_enriched AS (
    SELECT
        oc.*,
        p.Category,
        s.seller_city,
        s.seller_state,
        s.Region
    FROM oc
    LEFT JOIN ProductsInfo AS p ON p.product_id = oc.product_id
    LEFT JOIN Stores_Info AS s ON s.StoreID = oc.Delivered_StoreID
),
-- Order-level time features used later for weekday/weekend & slots
oc_time AS (
    SELECT
        *,
        CASE WHEN DATENAME(weekday, bill_ts) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS is_weekend,
        CASE
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 6  AND 11 THEN '06-12'
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 12 AND 17 THEN '12-18'
            WHEN DATEPART(HOUR, bill_ts) BETWEEN 18 AND 23 THEN '18-24'
            ELSE '00-06'
        END AS time_slot
    FROM oc_enriched
),
-- Bring in payments (wide ? per-order aggregates + indicators)
pay_order AS (
    SELECT
        op.order_id,
        ISNULL(op.Voucher,0)     AS amt_voucher,
        ISNULL(op.UPI_Cash,0)    AS amt_upi,
        ISNULL(op.Credit_Card,0) AS amt_credit,
        ISNULL(op.Debit_Card,0)  AS amt_debit,
        /* how many payment types used in the order (non-zero columns) */
        (CASE WHEN ISNULL(op.Voucher,0)     > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.UPI_Cash,0)    > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.Credit_Card,0) > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.Debit_Card,0)  > 0 THEN 1 ELSE 0 END) AS pay_types_used
    FROM order_payment AS op
),
-- Combine orders with payments and customer master
base AS (
    SELECT
        t.Customer_id,
        t.order_id,
        t.product_id,
        t.Channel,
        t.Delivered_StoreID,
        t.bill_ts,
        t.Quantity,
        t.Cost_Per_Unit,
        t.MRP,
        t.Discount,
        t.Total_Amount,
        t.profit,
        t.Category,
        t.seller_city,
        t.seller_state,
        t.Region,
        t.is_weekend,
        t.time_slot,
        c.customer_city,
        c.customer_state,
        c.Gender,
        ISNULL(p.amt_voucher,0) AS amt_voucher,
        ISNULL(p.amt_upi,0)     AS amt_upi,
        ISNULL(p.amt_credit,0)  AS amt_credit,
        ISNULL(p.amt_debit,0)   AS amt_debit,
        ISNULL(p.pay_types_used,0) AS pay_types_used
    FROM oc_time AS t
    LEFT JOIN Customer AS c
        ON c.Custid = t.Customer_id
    LEFT JOIN pay_order AS p
        ON p.order_id = t.order_id
),
-- Customer-level payment totals (also used to compute preferred method)
pay_cust AS (
    SELECT
        Customer_id,
        SUM(amt_voucher) AS pay_voucher,
        SUM(amt_upi)     AS pay_upi,
        SUM(amt_credit)  AS pay_credit,
        SUM(amt_debit)   AS pay_debit
    FROM base
    GROUP BY Customer_id
),
-- Preferred payment method via UNPIVOT + window rank
pref_method AS (
    SELECT Customer_id, method, total_amt,
           ROW_NUMBER() OVER (PARTITION BY Customer_id ORDER BY total_amt DESC, method) AS rn
    FROM (
        SELECT Customer_id, pay_voucher, pay_upi, pay_credit, pay_debit FROM pay_cust
    ) d
    UNPIVOT (total_amt FOR method IN (pay_voucher, pay_upi, pay_credit, pay_debit)) u
),
-- Global max date for recency
max_dt AS (
    SELECT MAX(bill_ts) AS max_bill_ts FROM base
),
-- Main customer 360 aggregation
cust_360 AS (
    SELECT
        b.Customer_id,
        MAX(b.customer_city)  AS customer_city,
        MAX(b.customer_state) AS customer_state,
        MAX(b.Gender)         AS Gender,

        MIN(b.bill_ts) AS First_Transaction_Date,
        MAX(b.bill_ts) AS Last_Transaction_Date,

        DATEDIFF(DAY, MIN(b.bill_ts), MAX(b.bill_ts)) AS Tenure_Days,

        COUNT(DISTINCT b.order_id) AS Distinct_Transactions,      -- frequency
        SUM(b.Total_Amount)        AS Total_Amount_Spent,         -- monetary
        SUM(b.profit)              AS Total_Profit,
        SUM(ISNULL(b.Discount,0))  AS Total_Discount,

        SUM(b.Quantity)            AS Total_Quantity,

        COUNT(DISTINCT b.product_id)     AS Distinct_Items_Purchased,
        COUNT(DISTINCT b.Category)       AS Distinct_Categories_Purchased,

        SUM(CASE WHEN ISNULL(b.Discount,0) > 0 THEN 1 ELSE 0 END) AS Transactions_With_Discount,
        SUM(CASE WHEN b.profit < 0 THEN 1 ELSE 0 END)             AS Transactions_With_Loss,

        COUNT(DISTINCT b.Channel)          AS Distinct_Channels_Used,
        COUNT(DISTINCT b.Delivered_StoreID) AS Distinct_Stores_Purchased,
        COUNT(DISTINCT b.seller_city)       AS Distinct_Cities_Purchased,

        SUM(b.pay_types_used) AS Different_Payment_Types_Used, -- across all orders

        SUM(CASE WHEN b.is_weekend = 0 THEN 1 ELSE 0 END) AS Txn_Weekdays,
        SUM(CASE WHEN b.is_weekend = 1 THEN 1 ELSE 0 END) AS Txn_Weekends,

        SUM(CASE WHEN b.time_slot = '06-12' THEN 1 ELSE 0 END) AS Txn_06_12,
        SUM(CASE WHEN b.time_slot = '12-18' THEN 1 ELSE 0 END) AS Txn_12_18,
        SUM(CASE WHEN b.time_slot = '18-24' THEN 1 ELSE 0 END) AS Txn_18_24,
        SUM(CASE WHEN b.time_slot = '00-06' THEN 1 ELSE 0 END) AS Txn_00_06
    FROM base AS b
    GROUP BY b.Customer_id
)
SELECT
    c.Customer_id,
    c.customer_city,
    c.customer_state,
    c.Gender,

    c.First_Transaction_Date,
    c.Last_Transaction_Date,
    c.Tenure_Days,

    -- Recency = max_date_in_data - last_transaction_date
    DATEDIFF(DAY, c.Last_Transaction_Date, m.max_bill_ts) AS Inactive_Days_Recency,

    c.Distinct_Transactions     AS Frequency_No_of_Transactions,
    c.Total_Amount_Spent        AS Monetary_Total_Revenue,
    c.Total_Profit,
    c.Total_Discount,
    c.Total_Quantity,

    c.Distinct_Items_Purchased,
    c.Distinct_Categories_Purchased,
    c.Transactions_With_Discount,
    c.Transactions_With_Loss,

    c.Distinct_Channels_Used,
    c.Distinct_Stores_Purchased,
    c.Distinct_Cities_Purchased,

    pc.pay_voucher AS Transactions_Paid_Using_Voucher,
    pc.pay_credit  AS Transactions_Paid_Using_CreditCard,
    pc.pay_debit   AS Transactions_Paid_Using_DebitCard,
    pc.pay_upi     AS Transactions_Paid_Using_UPI,

    c.Different_Payment_Types_Used,

    pm.method AS Preferred_Payment_Method,  -- values like pay_voucher/pay_credit...
    c.Txn_Weekdays,
    c.Txn_Weekends,
    c.Txn_06_12,
    c.Txn_12_18,
    c.Txn_18_24,
    c.Txn_00_06 into Customer360
FROM cust_360 AS c
JOIN max_dt   AS m  ON 1 = 1
LEFT JOIN pay_cust AS pc ON pc.Customer_id = c.Customer_id
LEFT JOIN pref_method AS pm
  ON pm.Customer_id = c.Customer_id AND pm.rn = 1
ORDER BY c.Customer_id


--------------------- Create Order360 Table -----------------

/* ===========================================
   ORDER 360 (Order-level) — ENHANCED VERSION
   =========================================== */

-- 1) Line level with costs, profit and time features
WITH line_lvl AS (
    SELECT
        o.order_id,
        o.product_id,
        o.Channel,
        o.Delivered_StoreID,
        CAST(o.Bill_date_timestamp AS datetime) AS bill_ts,
        o.Quantity,
        o.Cost_Per_Unit,
        o.MRP,
        ISNULL(o.Discount,0)            AS Discount,
        ISNULL(o.Total_Amount,0)        AS Total_Amount,      -- net revenue (after discounts)
        (o.Cost_Per_Unit * o.Quantity)  AS line_cost,
        (ISNULL(o.Total_Amount,0) - (o.Cost_Per_Unit * o.Quantity)) AS line_profit,
        (o.MRP * o.Quantity)            AS line_gross_rev
    FROM dbo.order_cleanf AS o
),
-- 2) Add product category
line_cat AS (
    SELECT
        l.*,
        p.Category
    FROM line_lvl AS l
    LEFT JOIN ProductsInfo AS p
      ON p.product_id = l.product_id
),
-- 3) Store attributes (city/state/region)
line_store AS (
    SELECT
        lc.*,
        s.seller_city,
        s.seller_state,
        s.Region
    FROM line_cat AS lc
    LEFT JOIN Stores_Info AS s
      ON s.StoreID = lc.Delivered_StoreID
),
-- 4) Order timestamp (use earliest row; change to MAX if you need latest)
order_time AS (
    SELECT
        order_id,
        MIN(bill_ts) AS order_ts,
        CASE WHEN DATENAME(weekday, MIN(bill_ts)) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS weekend_flag,
        CASE
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 6  AND 11 THEN '06-12'
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 12 AND 17 THEN '12-18'
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 18 AND 23 THEN '18-24'
            ELSE '00-06'
        END AS hours_flag
    FROM line_store
    GROUP BY order_id
),
-- 5) Primary channel/store/region (mode per order)
mode_rank AS (
    SELECT
        order_id,
        Channel,
        seller_city,
        Region,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COUNT(*) DESC, Channel) AS rn_ch,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COUNT(*) DESC, seller_city) AS rn_city,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COUNT(*) DESC, Region) AS rn_reg
    FROM line_store
    GROUP BY order_id, Channel, seller_city, Region
),
primary_dims AS (
    SELECT
        m.order_id,
        MAX(CASE WHEN rn_ch  = 1 THEN Channel     END) AS primary_channel,
        MAX(CASE WHEN rn_city= 1 THEN seller_city END) AS primary_store_city,
        MAX(CASE WHEN rn_reg = 1 THEN Region      END) AS primary_region
    FROM mode_rank AS m
    GROUP BY m.order_id
),
-- 6) Payment mix (wide table already)
pay_order AS (
    SELECT
        op.order_id,
        ISNULL(op.Voucher,0)     AS pay_voucher,
        ISNULL(op.UPI_Cash,0)    AS pay_upi,
        ISNULL(op.Credit_Card,0) AS pay_credit,
        ISNULL(op.Debit_Card,0)  AS pay_debit,
        (CASE WHEN ISNULL(op.Voucher,0)     > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.UPI_Cash,0)    > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.Credit_Card,0) > 0 THEN 1 ELSE 0 END +
         CASE WHEN ISNULL(op.Debit_Card,0)  > 0 THEN 1 ELSE 0 END) AS payment_types_used
    FROM order_payment AS op
),
-- 7) Ratings
ratings AS (
    SELECT
        r.order_id,
        r.cust_sat_avg_rating
    FROM orderreviews_ratings AS r
),
-- 8) Top category per order (by quantity; tie-break by name)
cat_rank AS (
    SELECT
        order_id,
        Category,
        SUM(Quantity) AS qty_in_cat,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY SUM(Quantity) DESC, Category) AS rn
    FROM line_store
    GROUP BY order_id, Category
),
top_cat AS (
    SELECT order_id, Category AS top_category
    FROM cat_rank
    WHERE rn = 1
),
-- 9) Aggregate per order
order_agg AS (
    SELECT
        l.order_id,
        COUNT(DISTINCT l.product_id)                  AS no_of_items,
        COUNT(DISTINCT l.Category)                    AS distinct_categories,
        COUNT(DISTINCT l.Channel)                     AS num_channels_used,
        COUNT(DISTINCT l.Delivered_StoreID)           AS num_stores_used,

        SUM(l.Quantity)                               AS qty,
        SUM(l.Total_Amount)                           AS net_revenue,        -- amount
        SUM(l.Discount)                               AS discount,
        SUM(CASE WHEN l.Discount > 0 THEN 1 ELSE 0 END) AS items_with_discount,

        SUM(l.line_cost)                              AS total_cost,
        SUM(l.line_gross_rev)                         AS gross_revenue,      -- MRP × Qty
        SUM(l.line_profit)                            AS margin_amount       -- same as total_profit
    FROM line_store AS l
    GROUP BY l.order_id
),
-- 10) Profit percentile for "high profit" flag
with_p75 AS (
    SELECT
        oa.*,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY oa.margin_amount)
        OVER () AS p75_profit
    FROM order_agg AS oa
)
SELECT
    w.order_id                                  AS Orderid,
    w.no_of_items                               AS [No.of items],
    w.qty                                       AS qty,
    w.net_revenue                               AS amount,
    w.discount                                  AS discount,
    w.items_with_discount                        AS [items with discount],
    w.total_cost                                 AS [total cost],
    w.margin_amount                              AS [total profit],
    CASE WHEN w.margin_amount < 0 THEN 1 ELSE 0 END          AS [Flag_loss_making],
    CASE WHEN w.margin_amount >= w.p75_profit THEN 1 ELSE 0 END AS [Orders with_high_profit],
    w.distinct_categories                        AS [distinct categories],
    ot.weekend_flag                              AS [weekend_trans_flag],
    ot.hours_flag                                AS [Hours_flag],

    -- ********* NEW COLUMNS *********
    w.gross_revenue,
    CAST(CASE WHEN w.qty > 0 THEN w.net_revenue * 1.0 / w.qty END AS decimal(18,4)) AS avg_item_price,
    CAST(CASE WHEN w.gross_revenue > 0 THEN w.discount * 1.0 / w.gross_revenue END AS decimal(18,4)) AS avg_discount_pct,
    CAST(CASE WHEN w.gross_revenue > 0 THEN w.margin_amount * 1.0 / w.gross_revenue END AS decimal(18,4)) AS margin_pct,

    w.num_channels_used,
    w.num_stores_used,
    pd.primary_channel,
    pd.primary_store_city,
    pd.primary_region,

    po.pay_voucher,
    po.pay_upi,
    po.pay_credit,
    po.pay_debit,
    po.payment_types_used,
    CASE WHEN po.payment_types_used > 1 THEN 1 ELSE 0 END AS split_payment_flag,

    rt.cust_sat_avg_rating,
    CASE
        WHEN rt.cust_sat_avg_rating >= 4.5 THEN 'Excellent'
        WHEN rt.cust_sat_avg_rating >= 3.5 THEN 'Good'
        WHEN rt.cust_sat_avg_rating >= 2.5 THEN 'Average'
        WHEN rt.cust_sat_avg_rating IS NULL THEN 'No Rating'
        ELSE 'Poor'
    END AS rating_band,

    tc.top_category,

    CASE
        WHEN w.net_revenue >= 2000 THEN 'High'
        WHEN w.net_revenue >= 800  THEN 'Mid'
        ELSE 'Low'
    END AS amount_band,

    CASE
        WHEN w.qty >= 20 THEN 'Bulk'
        WHEN w.qty >= 6  THEN 'Medium'
        ELSE 'Small'
    END AS qty_band
	into Order360
FROM with_p75   AS w
JOIN order_time AS ot ON ot.order_id = w.order_id
LEFT JOIN primary_dims AS pd ON pd.order_id = w.order_id
LEFT JOIN pay_order    AS po ON po.order_id = w.order_id
LEFT JOIN ratings      AS rt ON rt.order_id = w.order_id
LEFT JOIN top_cat      AS tc ON tc.order_id = w.order_id
ORDER BY w.order_id

--------------------- Create Store360 Table --------------------

/* ===========================================
   STORE 360 - STORE LEVEL PERFORMANCE SUMMARY
   =========================================== */

-- 1?? Line level: compute cost, profit, and revenue
WITH line_lvl AS (
    SELECT
        o.Delivered_StoreID        AS StoreID,
        o.order_id,
        o.Customer_id,
        o.product_id,
        CAST(o.Bill_date_timestamp AS datetime) AS bill_ts,
        o.Quantity,
        o.Cost_Per_Unit,
        o.MRP,
        ISNULL(o.Discount,0) AS Discount,
        ISNULL(o.Total_Amount,0) AS Total_Amount,
        (o.Cost_Per_Unit * o.Quantity) AS line_cost,
        (ISNULL(o.Total_Amount,0) - (o.Cost_Per_Unit * o.Quantity)) AS line_profit
    FROM order_cleanf AS o
),

-- 2?? Add product category and store location
line_enriched AS (
    SELECT
        l.*,
        p.Category,
        s.seller_city     AS Location,
        s.Region          AS Region
    FROM line_lvl AS l
    LEFT JOIN ProductsInfo AS p
      ON p.product_id = l.product_id
    LEFT JOIN Stores_Info AS s
      ON s.StoreID = l.StoreID
),

-- 3?? Time segmentation (weekday/weekend, hours)
time_flags AS (
    SELECT
        order_id,
        StoreID,
        MIN(bill_ts) AS bill_ts,
        CASE WHEN DATENAME(weekday, MIN(bill_ts)) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS weekend_flag,
        CASE
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 6  AND 11 THEN '06-12'
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 12 AND 17 THEN '12-18'
            WHEN DATEPART(HOUR, MIN(bill_ts)) BETWEEN 18 AND 23 THEN '18-24'
            ELSE '00-06'
        END AS hours_flag
    FROM line_enriched
    GROUP BY order_id, StoreID
),

-- 4?? Order-level aggregation (for profit flags)
order_profit AS (
    SELECT
        StoreID,
        order_id,
        SUM(ISNULL(Total_Amount,0)) - SUM(Cost_Per_Unit * Quantity) AS order_profit
    FROM line_enriched
    GROUP BY StoreID, order_id
),
p75_order_profit AS (
    SELECT DISTINCT
        StoreID,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY order_profit)
        OVER (PARTITION BY StoreID) AS p75_profit
    FROM order_profit
),

-- 5?? Main Store-level metrics
store_agg AS (
    SELECT
        l.StoreID,
        MAX(l.Location)                          AS Location,
        COUNT(DISTINCT l.product_id)             AS No_of_items,
        SUM(l.Quantity)                          AS qty,
        SUM(l.Total_Amount)                      AS amount,
        SUM(l.Discount)                          AS discount,
        SUM(CASE WHEN l.Discount > 0 THEN 1 ELSE 0 END) AS items_with_discount,
        SUM(l.line_cost)                         AS total_cost,
        SUM(l.line_profit)                       AS total_profit,
        COUNT(DISTINCT l.Category)               AS distinct_categories,
        COUNT(DISTINCT l.Customer_id)            AS distinct_customers,

        SUM(CASE WHEN l.line_profit < 0 THEN 1 ELSE 0 END) AS Flag_loss_making,
        SUM(CASE WHEN op.order_profit >= p75.p75_profit THEN 1 ELSE 0 END) AS Orders_with_high_profit,

        SUM(CASE WHEN tf.weekend_flag = 1 THEN 1 ELSE 0 END) AS weekend_trans_flag,
        SUM(CASE WHEN tf.weekend_flag = 0 THEN 1 ELSE 0 END) AS weekday_trans_flag,
        COUNT(DISTINCT tf.hours_flag)            AS Hours_flag,

        SUM(CASE WHEN tf.weekend_flag = 1 THEN l.Total_Amount ELSE 0 END) AS weekend_sales,
        SUM(CASE WHEN tf.weekend_flag = 0 THEN l.Total_Amount ELSE 0 END) AS weekday_sales
    FROM line_enriched AS l
    LEFT JOIN time_flags AS tf
        ON tf.order_id = l.order_id AND tf.StoreID = l.StoreID
    LEFT JOIN order_profit AS op
        ON op.order_id = l.order_id AND op.StoreID = l.StoreID
    LEFT JOIN p75_order_profit AS p75
        ON p75.StoreID = l.StoreID
    GROUP BY l.StoreID
),

-- 6?? Bring in order & review counts
order_counts AS (
    SELECT
        o.Delivered_StoreID AS StoreID,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM dbo.order_cleanf AS o
    GROUP BY o.Delivered_StoreID
),
ratings AS (
    SELECT
        o.Delivered_StoreID AS StoreID,
        AVG(r.cust_sat_avg_rating) AS avg_rating_per_customer
    FROM order_cleanf AS o
    JOIN orderreviews_ratings AS r
        ON r.order_id = o.order_id
    GROUP BY o.Delivered_StoreID
)

-- 7?? Final Store 360 Output
SELECT
    sa.StoreID,
    sa.Location,
    sa.No_of_items,
    sa.qty,
    sa.amount,
    sa.discount,
    sa.items_with_discount,
    sa.total_cost,
    sa.total_profit,
    sa.Flag_loss_making,
    sa.Orders_with_high_profit,
    sa.distinct_categories,
    sa.weekend_trans_flag,
    sa.Hours_flag,
    sa.weekend_sales,
    sa.weekday_sales,

    -- Derived averages
    CAST(CASE WHEN oc.total_orders > 0 THEN sa.amount * 1.0 / oc.total_orders END AS decimal(18,2)) AS [Average order value],
    CAST(CASE WHEN oc.total_orders > 0 THEN sa.total_profit * 1.0 / oc.total_orders END AS decimal(18,2)) AS [Average profit per transaction],
    CAST(CASE WHEN sa.distinct_customers > 0 THEN sa.total_profit * 1.0 / sa.distinct_customers END AS decimal(18,2)) AS [Average profit per customer],
    CAST(CASE WHEN oc.total_orders > 0 THEN sa.distinct_customers * 1.0 / oc.total_orders END AS decimal(18,2)) AS [Average customer visits],
    r.avg_rating_per_customer AS [Average rating per customer]
	into Store360
FROM store_agg AS sa
LEFT JOIN order_counts AS oc ON oc.StoreID = sa.StoreID
LEFT JOIN ratings AS r ON r.StoreID = sa.StoreID
ORDER BY sa.StoreID


exec sp_columns Order360 