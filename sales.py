import sys
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col

# date_str = sys.argv[0]
# file_input = sys.argv[1]

tomorrow = datetime.now() + timedelta(1)
date_str = tomorrow.strftime("%Y%m%d")


spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()

inventory_df = (
    spark.read.option("header", "true")
    .option("delimiter", ",")
    .csv(f"s3://mid-term-wh-dump/inventory_{date_str}.csv.gz")
    # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
    # .csv(f"{file_input}")
)
sales_df = (
    spark.read.option("header", "true")
    .option("delimiter", ",")
    .csv(f"s3://mid-term-wh-dump/sales_{date_str}.csv.gz")
    # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
    # .csv(f"{file_input}")
)

calendar_df = (
    spark.read.option("header", "true")
    .option("delimiter", ",")
    .csv(f"s3://mid-term-wh-dump/calendar_{date_str}.csv.gz")
    # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
    # .csv(f"{file_input}")
)

store_df = (
    spark.read.option("header", "true")
    .option("delimiter", ",")
    .csv(f"s3://mid-term-wh-dump/store_{date_str}.csv.gz")
    # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
    # .csv(f"{file_input}")
)

product_df = (
    spark.read.option("header", "true")
    .option("delimiter", ",")
    .csv(f"s3://mid-term-wh-dump/product_{date_str}.csv.gz")
    # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
    # .csv(f"{file_input}")
)

# sales_inv_store_dy_df = (
#     spark.read.option("header", "true")
#     .option("delimiter", ",")
#     .csv(f"s3://mid-term-wh-dump/sales_inv_store_dy_{date_str}.csv.gz")
#     # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
#     # .csv(f"{file_input}")
# )

# sales_inv_store_wk_df = (
#     spark.read.option("header", "true")
#     .option("delimiter", ",")
#     .csv(f"s3://mid-term-wh-dump/sales_inv_store_wk_{date_str}.csv.gz")
#     # .csv(f"file://///home/ec2-user/midterm/data/sales_20221015.csv.gz")
#     # .csv(f"{file_input}")
# )


sales_df.createOrReplaceTempView("sales")
inventory_df.createOrReplaceTempView("inventory")
calendar_df.createOrReplaceTempView("calendar")
store_df.createOrReplaceTempView("store")
product_df.createOrReplaceTempView("product")
# sales_inv_store_dy_df.createOrReplaceTempView("sales_inv_store_dy_df")
# sales_inv_store_wk_df.createOrReplaceTempView("sales_inv_store_wk_df")


df_sales_inv_store_dy = spark.sql(
    """
    SELECT \
	s.TRANS_DT, \
	s.STORE_KEY, \
	s.PROD_KEY, \
	s.SALES_QTY, \
	s.SALES_PRICE, \
	s.SALES_AMT, \
	s.DISCOUNT, \
	s.SALES_COST, \
	s.SALES_MGRN, \
	i.INVENTORY_ON_HAND_QTY, \
	i.INVENTORY_ON_ORDER_QTY, \
	i.OUT_OF_STOCK_FLG, \
	CASE WHEN i.OUT_OF_STOCK_FLG=TRUE THEN FALSE ELSE TRUE END AS in_stock_flg, \
	CASE WHEN i.INVENTORY_ON_HAND_QTY<s.SALES_QTY THEN TRUE ELSE FALSE END AS low_stock_flg \
FROM sales s \
FULL OUTER JOIN inventory i ON s.TRANS_DT=i.CAL_DT and s.STORE_KEY=i.STORE_KEY and s.PROD_KEY=i.PROD_KEY"""
)

df_sales_inv_store_dy.createOrReplaceTempView("sales_inv_store_wk")

# df_sales_inv_store_wk = spark.sql("SELECT * FROM df_sales_inv_store_dy")
# df_sales_inv_store_dy = spark.sql("SELECT * FROM sales_inv_store_dy_df")


# df_inventory = spark.sql(
#     "select i.store_key,i.prod_key, \
# sum(i.inventory_on_hand_qty) as sum_inventory_on_hand_qty, \
# sum(i.inventory_on_order_qty) as sum_inventory_on_order_qty, \
# c.day_of_wk_num, sum(i.out_of_stock_flg) as low_stock_impact, \
# sum(i.out_of_stock_flg)/7 as percentage_in_stock \
# from inventory i inner join calendar c on i.cal_dt==c.cal_dt group by 1,2,5;"
# )

# df_sales = spark.sql(
#     "select s.store_key,s.prod_key, \
#     sum(s.sales_qty) as sum_sales_qty, \
#     sum(s.sales_amt) as sum_sales_amt,  \
#     sum(s.sales_amt)/sum(s.sales_qty) as average_sales_price, \
#     c.day_of_wk_num, sum(s.sales_cost) as total_cost \
#     from sales s inner join calendar c on s.trans_dt==c.cal_dt group by 1,2,6;"
# )

df_sales_inv_store_wk = spark.sql(
    """
SELECT
	c.yr_num as yr_num,
	c.wk_num as wk_num,
	s.store_key,
	s.prod_key,
	sum(sales_qty) as wk_sales_qty,
	avg(sales_price) as avg_sales_price,
	sum(sales_amt) as wk_sales_amt,
	sum(discount) as wk_discount,
	sum(sales_cost) as wk_sales_cost,
	sum(sales_mgrn) as wk_sales_mgrn,
	sum(case when c.day_of_wk_num=6 then s.inventory_on_hand_qty else 0 end) as eop_stock_on_hand_qty,
	sum(case when c.day_of_wk_num=6 then s.inventory_on_order_qty else 0 end) as eop_ordered_stock_qty,
	count(case when s.out_of_stock_flg=true then 1 else 0 end) as out_of_stock_times,
	count(case when s.in_stock_flg=true then 1 else 0 end) as in_stock_times,
	count(case when s.low_stock_flg=true then 1 else 0 end) as low_stock_times
FROM sales_inv_store_wk s
JOIN calendar c on s.trans_dt=c.cal_dt
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;"""
)

df_calendar = spark.sql("SELECT * from calendar;")
df_store = spark.sql("SELECT * from store;")
df_product = spark.sql("SELECT * from product;")
# df_inventory.show()

# df_sales.show()

# df_sales_inv_store_dy.show()
df_sales_inv_store_wk.show()
# df_sales_inv_store_dy.show()
df_calendar.show()
df_product.show()
df_store.show()

# (
#     df_sales.repartition(1)
#     .write.mode("overwrite")
#     .option("compression", "gzip")
#     .parquet(f"s3://midterm-parquet/sales/{date_str}")
#     # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
# )

(
    df_calendar.repartition(1)
    .write.mode("overwrite")
    .option("compression", "gzip")
    .parquet(f"s3://midterm-parquet/calendar/{date_str}")
    # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
)

(
    df_product.repartition(1)
    .write.mode("overwrite")
    .option("compression", "gzip")
    .parquet(f"s3://midterm-parquet/product/{date_str}")
    # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
)

(
    df_store.repartition(1)
    .write.mode("overwrite")
    .option("compression", "gzip")
    .parquet(f"s3://midterm-parquet/store/{date_str}")
    # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
)

(
    df_sales_inv_store_wk.repartition(1)
    .write.mode("overwrite")
    .option("compression", "gzip")
    .parquet(f"s3://midterm-parquet/sales_inv_store_wk/{date_str}")
    # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
)

# (
#     df_sales_inv_store_dy.repartition(1)
#     .write.mode("overwrite")
#     .option("compression", "gzip")
#     .parquet(f"s3://midterm-parquet/sales_inv_store_dy/{date_str}")
#     # .parquet(f"file://///home/ec2-user/midterm/result/date={date_str}")
# )
