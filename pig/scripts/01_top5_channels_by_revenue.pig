-- BM Sales Pig Analysis
-- Input columns:
-- date, store_id, sku_id, customer_id, quantity, unit_price, total_value, channel, discount_pct
sales = LOAD '/user/HP/BM_Sales_Hadoop_Pig_Project/dataset/bm_sales.csv' USING PigStorage(',')
    AS (date:chararray, store_id:chararray, sku_id:chararray,
        customer_id:chararray, quantity:int, unit_price:double,
        total_value:double, channel:chararray, discount_pct:double);

clean = FILTER sales BY date != 'date';

grp = GROUP clean BY channel;
agg = FOREACH grp GENERATE group AS channel,
    SUM(clean.total_value) AS total_revenue;
sorted = ORDER agg BY total_revenue DESC;
top5 = LIMIT sorted 5;
STORE top5 INTO '/user/HP/BM_Sales_Hadoop_Pig_Project/output/pig/pig1_top5_channels' USING PigStorage(',');

