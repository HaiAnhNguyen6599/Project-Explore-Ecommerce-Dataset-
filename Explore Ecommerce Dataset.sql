/*Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
-- total visist, transactions, total pageviews 
-- Jan, Feb, March 2017 */ 

SELECT 
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month, -- cot date datatype string
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where 
--Format_date('%Y%m', parse_date('%Y%m%d',date)) in ('201701', '201702', '201703') -- Jan, Feb, March
_TABLE_SUFFIX BETWEEN '0101' AND '0331'  -- 01/01/2017 - 31/03/2017
group by month
order by month;


-------------------------------------
/*Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
-- bonce_num
-- total_visit
-- July 7 */


SELECT  
  distinct trafficsource.source as source, --source
  sum(totals.visits) as total_visit, -- total_visist each source
  sum(totals.bounces) as total_no_of_bounces, -- total bounce in each source
  100.0 * sum(totals.bounces)/sum(totals.visits)  as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` -- July 7 2017
group by source
order by total_visit desc;





---------------------------

/*Query 3: Revenue by traffic source by week, by month in June 2017
 -- week, month
 -- June 2017
 -- do product Revenue dang o dang array nen phai dung unnest de lay du lieu */

with monthly_revenue as
(
select 
  'Month' as time_type,
  format_date('%Y%m', parse_date('%Y%m%d', date)) as time, -- lay time duoi dang yearmonth 201706
  trafficsource.source as source,
  sum(product.productRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
unnest (hits) as hits, -- unnest hits truoc
unnest (hits.product) as product -- sau do unnest hits.product
where product.productRevenue is not null
group by source, time
)
,
weekly_revenue as -- revenue theo week
(
select 
  'Week' as time_type,
  format_date('%Y%W', parse_date('%Y%m%d', date)) as time, -- lay time duoi dang yearmonth 201706
  trafficsource.source as source,
  sum(product.productRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
unnest (hits) as hits, -- unnest hits truoc
unnest (hits.product) as product -- sau do unnest hits.product
where product.productRevenue is not null
group by source, time
)

-- Union all de ra tat ca cac ket qua
-- va order theo time,source de de dang quan sat cac ket qua theo source cung khaong thoi gian cua no
select *
from monthly_revenue
union all
select *
from weekly_revenue
order by source, time;






------------

/* Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
-- purchasers vs non-purchasers
-- avg number of pageviews = totalpageview / number of unique userid 
-- June and July 2017 */

-- purchaser thi totals.transaction >= 1 and product revenue is not null
with purchasers as 
(
select
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  -- count(distinct fullVisitorId) as unique_userid, -- unique user
  sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageview_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest (hits) hits,
unnest (hits.product) as product
where product.productRevenue is not null -- dieu kieu cua purchasers
and totals.transactions >=1 -- dieu kien purchasers
and Format_date('%Y%m', parse_date('%Y%m%d',date)) in ('201706', '201707') -- June, July 2017
group by month
order by avg_pageview_purchase
)
,
-- purchaser thi totals.transaction null and product revenue is null
non_purchasers as
(
select
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  -- count(distinct fullVisitorId) as unique_userid,
  sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageview_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest (hits) hits,
unnest (hits.product) as product
where product.productRevenue is null -- dieu kien non_purchasers
and totals.transactions is null -- dieu kien non_purchaser
and Format_date('%Y%m', parse_date('%Y%m%d',date)) in ('201706', '201707')
group by month
order by avg_pageview_non_purchase 
)

-- left join de mo rong theo chieu ngang
select p.month, p.avg_pageview_purchase, np.avg_pageview_non_purchase
from purchasers as p
left join non_purchasers as np
on np.month = p.month
order by p.month;





---------------------

/* Query 05: Average number of transactions per user that made a purchase in July 2017
-- get totals transaction
-- get unique user
-- totals transaction / unique user 
-- July 217 */

select
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  -- count(distinct fullVisitorId) as unique_userid, -- unique user
  sum(totals.transactions) / count(distinct fullVisitorId) as avg_total_transaction_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, -- July 2017
unnest (hits) hits,
unnest (hits.product) as product
where product.productRevenue is not null -- dieu kieu cua purchasers
and totals.transactions >=1 -- dieu kien purchasers
group by month;





-----------------
-- Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
-- July 2017
-- total revenue / total visit

select 
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  round(sum(productRevenue) / sum(totals.visits) / (1000000),2) as avg_revenue_by_user_per_visit, -- Chia cho 1m do gia dang dc multiplier by 1m
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, -- July 2017
unnest (hits) hits,
unnest (hits.product) as product
where product.productRevenue is not null -- dieu kieu cua purchasers
and totals.transactions is not null -- dieu kien purchasers
group by month;





-------------
-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
-- Other Product Name (Find poeple buy V2)
-- Quantity of Other Prodyct
-- July 2017

-- B1: Tim nhung nguoi da mua  v2Product Name = "YouTube Men's Vintage Henley"
with v2_purchasers as 
(
select
  fullvisitorid
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, -- July 2017
Unnest (hits) as hits,
unnest (hits.product) as product 
where product.productrevenue is not null -- avoid duplicate
and product.v2ProductName = "YouTube Men's Vintage Henley"
)

-- B2: Tu CTE da co ket qua nhung nguoi mua "YouTube Men's Vintage Henley", now output ra cac user mua "YouTube Men's Vintage Henley", v2Name se loai di "YouTube Men's Vintage Henley"
select 
  product.v2ProductName as name,
  sum(product.productQuantity) as quantity,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, -- July 2017
Unnest (hits) as hits,
unnest (hits.product) as product 
where fullvisitorid in (  -- dieu kien fullvisitorid da mua v2_product o CTE tren
        select * from v2_purchasers)
and product.v2ProductName != "YouTube Men's Vintage Henley" -- loai tru V2name = "YouTube Men's Vintage Henley" de out cai san pham con lai
and product.productrevenue is not null -- avoid null
group by name
order by quantity desc;


-------------------
/* "Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level."



/* B1: dem cai loai action type 
actiontype = 2: num_product_view
actiontype = 3: add_to_cart */ 


/* B2: Tinh rieng num_purchase 
do co dieu kien product.productRevenue is not null de avoid duplicate
actiontype = 6: num_purchase */



/* B3: Tinh rate
left join 2 cte tren theo month: Cte 1 left 
add_to_cart_rate = add_to_cart/num_product_view
purchase_rate = num_purchase.num_product_view */


with product_data as(
select
  Format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  SUM(case when eCommerceAction.action_type = '2' then 1 else 0 end) as num_product_view, -- action_type dang o dang string
  SUM(case when eCommerceAction.action_type = '3' then 1 else 0 end) as num_add_to_cart,
  sum(case when eCommerceAction.action_type = '6' and product.ProductRevenue is not null then 1 else 0 end) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
Unnest (hits) as hits,
unnest (hits.product) as product
where _table_suffix between '20170101' and '20170331' -- Jan, Feb, March 2017
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;






