from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Create Spark session with Kafka integration"""
    return SparkSession.builder \
        .appName("EcommerceRealTimeAnalytics") \
        .master("local[*]")  \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")  \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_schemas():
    """Define schemas for different event types"""
    
    # Base event schema
    base_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    # Page view schema
    pageview_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("product_brand", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("time_on_page", IntegerType(), True)
    ])
    
    # Purchase schema
    purchase_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("total_items", IntegerType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("tax", DoubleType(), True),
        StructField("shipping", DoubleType(), True),
        StructField("total", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("items", ArrayType(StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])), True)
    ])
    
    return {
        "base": base_schema,
        "pageview": pageview_schema,
        "purchase": purchase_schema
    }

def read_kafka_stream(spark, topic):
    """Read streaming data from Kafka topic"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def process_pageviews(spark, schemas):
    """Process page view events for real-time analytics"""
    
    # Read pageview stream
    pageview_stream = read_kafka_stream(spark, "ecommerce-pageviews")
    
    # Parse JSON and apply schema
    pageview_parsed = pageview_stream.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schemas["pageview"]).alias("data")
    ).select("kafka_timestamp", "data.*")
    
    # Convert timestamp to proper format
    pageview_processed = pageview_parsed.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    # Real-time analytics: Popular products by category
    popular_products = pageview_processed \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("product_category"),
            col("product_name")
        ) \
        .agg(
            count("*").alias("view_count"),
            avg("product_price").alias("avg_price"),
            countDistinct("user_id").alias("unique_viewers")
        ) \
        .orderBy(desc("view_count"))
    
    return popular_products

def process_purchases(spark, schemas):
    """Process purchase events for revenue analytics"""
    
    # Read purchase stream
    purchase_stream = read_kafka_stream(spark, "ecommerce-purchases")
    
    # Parse JSON and apply schema
    purchase_parsed = purchase_stream.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schemas["purchase"]).alias("data")
    ).select("kafka_timestamp", "data.*")
    
    # Convert timestamp
    purchase_processed = purchase_parsed.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    # Real-time revenue analytics
    revenue_analytics = purchase_processed \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute")
        ) \
        .agg(
            count("*").alias("total_orders"),
            sum("total").alias("total_revenue"),
            avg("total").alias("avg_order_value"),
            sum("total_items").alias("total_items_sold"),
            countDistinct("user_id").alias("unique_customers")
        )
    
    return revenue_analytics

def calculate_conversion_metrics(spark, schemas):
    """Calculate real-time conversion metrics"""
    
    # Read multiple streams
    pageview_stream = read_kafka_stream(spark, "ecommerce-pageviews")
    purchase_stream = read_kafka_stream(spark, "ecommerce-purchases")
    cart_stream = read_kafka_stream(spark, "ecommerce-cart-events")
    
    # Parse pageviews
    pageviews = pageview_stream.select(
        from_json(col("value").cast("string"), schemas["pageview"]).alias("data")
    ).select("data.*").withColumn(
        "event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    # Parse purchases
    purchases = purchase_stream.select(
        from_json(col("value").cast("string"), schemas["purchase"]).alias("data")
    ).select("data.*").withColumn(
        "event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    
    # Calculate conversion rates by time window
    pageview_counts = pageviews \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
        .agg(countDistinct("user_id").alias("unique_viewers"))
    
    purchase_counts = purchases \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
        .agg(countDistinct("user_id").alias("unique_buyers"))
    
    # Join to calculate conversion rate
    conversion_rate = pageview_counts.join(
        purchase_counts, 
        ["window"], 
        "left_outer"
    ).select(
        col("window"),
        col("unique_viewers"),
        coalesce(col("unique_buyers"), lit(0)).alias("unique_buyers"),
        (coalesce(col("unique_buyers"), lit(0)) / col("unique_viewers") * 100).alias("conversion_rate_percent")
    )
    
    return conversion_rate

def start_streaming_queries(spark):
    """Start all streaming queries"""
    
    schemas = define_schemas()
    
    # Popular products query
    popular_products = process_pageviews(spark, schemas)
    popular_products_query = popular_products.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime="30 seconds") \
        .queryName("popular_products") \
        .start()
    
    # Revenue analytics query
    revenue_analytics = process_purchases(spark, schemas)
    revenue_query = revenue_analytics.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .queryName("revenue_analytics") \
        .start()
    
    # Conversion rate query
    conversion_metrics = calculate_conversion_metrics(spark, schemas)
    conversion_query = conversion_metrics.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .queryName("conversion_metrics") \
        .start()
    
    return [popular_products_query, revenue_query, conversion_query]

def main():
    """Main streaming application"""
    
    print("🚀 Starting E-commerce Real-Time Analytics...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
    
    print("✅ Spark session created successfully!")
    print(f"📊 Spark UI available at: http://localhost:8090")
    print("📈 Starting real-time analytics queries...")
    
    try:
        # Start streaming queries
        queries = start_streaming_queries(spark)
        
        print("🔄 Streaming queries started! Waiting for data...")
        print("📱 Check Kafka UI at http://localhost:8080 to see incoming events")
        print("🎯 Analytics will appear below as events are processed...")
        print("-" * 80)
        
        # Wait for all queries to complete
        for query in queries:
            query.awaitTermination()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    main()