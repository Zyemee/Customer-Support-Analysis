SELECT *
FROM customer_support_data
ORDER BY order_date_time DESC
LIMIT 20
;

CREATE TABLE customer_support_data_staging
SELECT *
FROM customer_support_data
;

INSERT INTO customer_support_data_staging
SELECT *
FROM customer_support_data
;

SELECT *
FROM customer_support_data_staging
;

-- clean column names

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Unique id` TO unique_id;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Sub-category` TO sub_category
;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Customer Remarks` TO customer_remarks
;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Issue_reported at` TO issue_reported_at
;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Tenure Bucket` TO tenure_bucket
;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `Agent Shift` TO agent_shift
;

ALTER TABLE customer_support_data_staging
RENAME COLUMN `CSAT Score` TO CSAT_Score
;

-- REMOVING DUPLICATES
SELECT *
FROM customer_support_data_staging2
;

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY unique_id, channel_name, category, sub_category, customer_remarks, Order_id, order_date_time, issue_reported_at, issue_responded, Survey_response_Date, 
Customer_City, Product_category, Item_price, connected_handling_time, Agent_name, Supervisor, Manager, tenure_bucket, agent_shift, CSAT_Score) AS row_num
FROM customer_support_data_staging
)

SELECT *
FROM duplicate_CTE
WHERE row_num > 1
;

CREATE TABLE customer_support_data_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY unique_id, channel_name, category, sub_category, customer_remarks, Order_id, order_date_time, issue_reported_at, issue_responded, Survey_response_Date, 
Customer_City, Product_category, Item_price, connected_handling_time, Agent_name, Supervisor, Manager, tenure_bucket, agent_shift, CSAT_Score) AS row_num
FROM customer_support_data_staging
;

DELETE
FROM customer_support_data_staging2
WHERE row_num > 1
;

-- Standardizing data
-- checking unique IDs & order_id length

SELECT unique_id, length(unique_id), order_id, length(order_id)
FROM customer_support_data_staging2
WHERE length(unique_id) > 36 OR length(unique_id) < 36
;

SELECT *, length(order_id)
FROM customer_support_data_staging2
WHERE length(order_id) > 36 OR length(order_id) < 36
;

SELECT *
FROM customer_support_data_staging2
LIMIT 10
;

--
UPDATE customer_support_data_staging2
SET issue_reported_at = STR_TO_DATE(issue_reported_at, '%d/%m/%Y %H:%i:%s')
;

UPDATE customer_support_data_staging2
SET issue_responded = STR_TO_DATE(issue_responded, '%d/%m/%Y %H:%i:%s')
;

ALTER TABLE customer_supporT_data_staging2
MODIFY COLUMN issue_reported_at DATETIME;

ALTER TABLE customer_support_data_staging2
MODIFY COLUMN issue_responded DATETIME;

-- DROP columns that will not be used
-- order_id, order_date_time, customer_city, connected_handling_time, row_num

ALTER TABLE customer_support_data_staging2
DROP COLUMN order_id
;

ALTER TABLE customer_support_data_staging2
DROP COLUMN customer_city
;

ALTER TABLE customer_support_data_staging2
DROP COLUMN connected_handling_time
;

ALTER TABLE customer_support_data_staging2
DROP COLUMN row_num
;