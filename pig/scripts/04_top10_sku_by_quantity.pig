-- BM Sales Pig Analysis
-- Input columns:
-- date, store_id, sku_id, customer_id, quantity, unit_price, total_value, channel, discount_pct
sales = LOAD 'hdfs:///user/HP/BM_Sales_Hadoop_Pig_Project/dataset/bm_sales.csv' USING PigStorage(',')
    AS (date:chararray, store_id:chararray, sku_id:chararray,
        customer_id:chararray, quantity:int, unit_price:double,
        total_value:double, channel:chararray, discount_pct:double);

clean = FILTER sales BY date != 'date';

grp = GROUP clean BY sku_id;
agg = FOREACH grp GENERATE group AS sku_id,
    SUM(clean.quantity) AS total_quantity;
sorted = ORDER agg BY total_quantity DESC;
top10 = LIMIT sorted 10;
STORE top10 INTO 'hdfs:///user/HP/BM_Sales_Hadoop_Pig_Project/output/pig/pig4_top10_sku_quantity' USING PigStorage(',');

