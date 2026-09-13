\# BM Sales Analysis Using Hadoop MapReduce and Apache Pig



\## Project Overview



This project analyzes the BM Sales dataset using:



\- Hadoop MapReduce

\- Apache Pig

\- HDFS

\- Java



The project follows the structure of the supplied YouTube Hadoop/Pig academic report, adapted to the BM Sales dataset.



\## Dataset



Dataset file:



`data/bm\_sales.csv`



Records:



641,843 transactions



Columns:



\- date

\- store\_id

\- sku\_id

\- customer\_id

\- quantity

\- unit\_price

\- total\_value

\- channel

\- discount\_pct



\## Hadoop MapReduce Operations



\### 1. Total Sales by Store



Calculates:



`store\_id -> SUM(total\_value)`



Java:



`StoreSalesSummary.java`



\### 2. Top 10 SKUs by Revenue



Calculates:



`sku\_id -> SUM(total\_value) -> Top 10`



Java:



`Top10SkuRevenue.java`



\### 3. Sales by Channel



Calculates:



`channel -> transaction count + total revenue`



Java:



`ChannelSalesSummary.java`



\## Apache Pig Operations



\### 1. Top 5 Channels by Revenue



\### 2. Top 10 SKUs by Revenue



\### 3. Top 10 Stores by Revenue



\### 4. Top 10 SKUs by Quantity



\### 5. Top 5 Channels by Average Transaction Value



\## Technology



\- Hadoop 3.4.0

\- OpenJDK 1.8.0\_492

\- Apache Pig 0.18.0

\- Windows



\## HDFS Dataset Path



`/user/HP/BM\_Sales\_Hadoop\_Pig\_Project/dataset/bm\_sales.csv`



\## HDFS Output Path



`/user/HP/BM\_Sales\_Hadoop\_Pig\_Project/output/`



\## Project Structure



See the project folders for Java MapReduce source, Pig scripts, results, documentation, and execution commands.

