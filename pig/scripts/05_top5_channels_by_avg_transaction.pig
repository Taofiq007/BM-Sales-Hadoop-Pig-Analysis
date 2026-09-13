-- BM Sales Pig Analysis
-- Input columns:
-- date, store_id, sku_id, customer_id, quantity, unit_price, total_value, channel, discount_pct
sales = LOAD 'hdfs:///user/HP/BM_Sales_Hadoop_Pig_Project/dataset/bm_sales.csv' USING PigStorage(',')
    AS (date:chararray, store_id:chararray, sku_id:chararray,
        customer_id:chararray, quantity:int, unit_price:double,
        total_value:double, channel:chararray, discount_pct:double);

clean = FILTER sales BY date != 'date';

grp = GROUP clean BY channel;
agg = FOREACH grp GENERATE group AS channel,
    COUNT(clean) AS transactions,
    SUM(clean.total_value) AS total_revenue;
with_avg = FOREACH agg GENERATE channel,
    transactions,
    total_revenue,
    (double)total_revenue / transactions AS avg_transaction;
sorted = ORDER with_avg BY avg_transaction DESC;
top5 = LIMIT sorted 5;
STORE top5 INTO 'hdfs:///user/HP/BM_Sales_Hadoop_Pig_Project/output/pig/pig5_avg_channel' USING PigStorage(',');

