from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Fecom_Ecommerce_Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark version:", spark.version)

# Đường dẫn tới thư mục chứa dữ liệu
DATA_PATH = "./data/"   

# Hàm tiện ích để đọc CSV với separator ';'
def read_csv(filename):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ";") \
        .csv(DATA_PATH + filename)

# Đọc tất cả các file
orders_df       = read_csv("Orders.csv")
customers_df    = read_csv("Customer_List.csv")
order_items_df  = read_csv("Order_Items.csv")
products_df     = read_csv("Products.csv")
reviews_df      = read_csv("Order_Reviews.csv")

# Kiểm tra schema
print("=== Orders ===")
orders_df.printSchema()

print("=== Customers ===")
customers_df.printSchema()

print("=== Order Items ===")
order_items_df.printSchema()

print("=== Products ===")
products_df.printSchema()

print("=== Order Reviews ===")
reviews_df.printSchema()

total_orders   = orders_df.select("Order_ID").distinct().count()
total_customers = customers_df.select("Subscriber_ID").distinct().count()
total_sellers  = order_items_df.select("Seller_ID").distinct().count()

print(f"Tổng số đơn hàng  : {total_orders:,}")
print(f"Số khách hàng duy nhất: {total_customers:,}")
print(f"Số người bán duy nhất : {total_sellers:,}")

orders_by_country = (
    orders_df
    .join(customers_df, on="Customer_Trx_ID", how="left")
    .groupBy("Customer_Country", "Customer_Country_Code")
    .agg(F.count("Order_ID").alias("Total_Orders"))
    .orderBy(F.col("Total_Orders").desc())
)

orders_by_country.show(30, truncate=False)

orders_by_ym = (
    orders_df
    .filter(F.col("Order_Purchase_Timestamp").isNotNull())
    .withColumn("Year",  F.year("Order_Purchase_Timestamp"))
    .withColumn("Month", F.month("Order_Purchase_Timestamp"))
    .groupBy("Year", "Month")
    .agg(F.count("Order_ID").alias("Total_Orders"))
    .orderBy(
        F.col("Year").asc(),
        F.col("Month").desc()
    )
)

orders_by_ym.show(50, truncate=False)

# Kiểm tra dữ liệu trước khi xử lý
print("--- Phân phối Review_Score trước khi làm sạch ---")
reviews_df.groupBy("Review_Score").count().orderBy("Review_Score").show()

print(f"Số giá trị NULL: {reviews_df.filter(F.col('Review_Score').isNull()).count()}")

# Chỉ giữ các điểm hợp lệ từ 1 đến 5
# Sử dụng F.expr("try_cast(...)") để gọi hàm của Spark SQL, tránh lỗi AttributeError
reviews_clean = reviews_df.withColumn(
    "Review_Score_Clean", 
    F.expr("try_cast(Review_Score as int)")
).filter(
    F.col("Review_Score_Clean").isNotNull() & 
    F.col("Review_Score_Clean").between(1, 5)
).drop("Review_Score").withColumnRenamed("Review_Score_Clean", "Review_Score")

print(f"\nSố bản ghi hợp lệ sau làm sạch: {reviews_clean.count():,}")
print(f"Số bản ghi bị loại           : {reviews_df.count() - reviews_clean.count():,}")

# Điểm trung bình tổng thể
avg_score = reviews_clean.agg(F.avg("Review_Score").alias("Avg_Score")).collect()[0]["Avg_Score"]
if avg_score is not None:
    print(f"\nĐiểm đánh giá trung bình tổng thể: {avg_score:.2f}")
else:
    print("\nKhông có điểm đánh giá hợp lệ (Vui lòng kiểm tra lại cấu trúc file CSV)")

# Thống kê theo từng mức điểm
print("\n--- Thống kê theo từng mức điểm ---")
review_stats = (
    reviews_clean
    .groupBy("Review_Score")
    .agg(
        F.count("Review_ID").alias("Count"),
        F.round(F.count("Review_ID") / reviews_clean.count() * 100, 2).alias("Percentage_%")
    )
    .orderBy("Review_Score")
)
review_stats.show()

# Lọc đơn hàng năm 2024
orders_2024 = orders_df.filter(
    F.year("Order_Purchase_Timestamp") == 2024
).select("Order_ID")

revenue_2024 = (
    orders_2024
    .join(order_items_df, on="Order_ID", how="inner")
    .join(products_df,    on="Product_ID", how="left")
    .withColumn("Revenue", F.col("Price") + F.col("Freight_Value"))
    .groupBy("Product_Category_Name")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("Total_Revenue"),
        F.count("Order_ID").alias("Total_Items_Sold")
    )
    .orderBy(F.col("Total_Revenue").desc())
)

print("=== Doanh thu năm 2024 theo danh mục sản phẩm ===")
revenue_2024.show(30, truncate=False)

# Tổng doanh thu
total_rev = revenue_2024.agg(F.sum("Total_Revenue")).collect()[0][0]
print(f"Tổng doanh thu năm 2024: {total_rev:,.2f}")

# Số lượng bán ra và điểm đánh giá trung bình theo Product_ID
product_sales = (
    order_items_df
    .groupBy("Product_ID")
    .agg(F.count("Order_Item_ID").alias("Units_Sold"))
)

product_reviews = (
    order_items_df
    .join(
        reviews_clean.select("Order_ID", "Review_Score"),
        on="Order_ID",
        how="left"
    )
    .groupBy("Product_ID")
    .agg(F.round(F.avg("Review_Score"), 2).alias("Avg_Review_Score"))
)

product_analysis = (
    product_sales
    .join(product_reviews, on="Product_ID", how="left")
    .join(products_df.select("Product_ID", "Product_Category_Name"), on="Product_ID", how="left")
    .orderBy(F.col("Units_Sold").desc())
)

print("=== Top 20 sản phẩm bán chạy nhất ===")
product_analysis.show(20, truncate=False)

# Sản phẩm bán nhiều nhất
top_product = product_analysis.first()
print(f"\nSản phẩm bán nhiều nhất:")
print(f"  Product_ID   : {top_product['Product_ID']}")
print(f"  Danh mục     : {top_product['Product_Category_Name']}")
print(f"  Số lượng bán : {top_product['Units_Sold']}")
print(f"  Điểm TB      : {top_product['Avg_Review_Score']}")

# Tính hiệu số ngày (dương = trễ, âm = sớm hơn hạn)
delivery_perf = (
    orders_df
    .filter(F.col("Order_Delivered_Carrier_Date").isNotNull())
    .join(
        order_items_df.select("Order_ID", "Shipping_Limit_Date"),
        on="Order_ID",
        how="inner"
    )
    .withColumn(
        "Delivery_Diff_Days",
        F.datediff(
            F.col("Order_Delivered_Carrier_Date"),
            F.col("Shipping_Limit_Date")
        )
    )
    .withColumn(
        "Delivery_Status",
        F.when(F.col("Delivery_Diff_Days") <= 0, "On-time / Early")
         .otherwise("Late")
    )
)

print("=== Mẫu dữ liệu hiệu suất giao hàng ===")
delivery_perf.select(
    "Order_ID",
    "Shipping_Limit_Date",
    "Order_Delivered_Carrier_Date",
    "Delivery_Diff_Days",
    "Delivery_Status"
).show(15, truncate=False)

# Tổng hợp thống kê
print("=== Thống kê hiệu suất giao hàng ===")
delivery_perf.groupBy("Delivery_Status") \
    .agg(
        F.count("Order_ID").alias("Order_Count"),
        F.round(F.avg("Delivery_Diff_Days"), 2).alias("Avg_Diff_Days"),
        F.min("Delivery_Diff_Days").alias("Min_Diff_Days"),
        F.max("Delivery_Diff_Days").alias("Max_Diff_Days")
    ) \
    .show(truncate=False)

# Tỷ lệ giao hàng đúng hạn
total = delivery_perf.count()
on_time = delivery_perf.filter(F.col("Delivery_Status") == "On-time / Early").count()
print(f"\nTỷ lệ giao hàng đúng hạn: {on_time}/{total} = {on_time/total*100:.1f}%")

spark.stop()
print("SparkSession đã dừng.")
