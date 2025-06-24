# Bulletproof PySpark Workshop - All Function Conflicts Resolved
# This version works without any import conflicts

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
from datetime import datetime, timedelta
import random
import builtins  # For Python built-in functions

# ==========================================
# SPARK SESSION SETUP
# ==========================================

def create_workshop_spark():
    """Create Spark session with proper configuration"""
    spark = SparkSession.builder \
        .appName("PySpark Workshop - Bulletproof") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ==========================================
# DATASET CREATION - SAFE VERSION
# ==========================================

def create_workshop_datasets():
    """Create datasets without any function conflicts"""
    print("📊 Creating workshop datasets...")
    
    # Create sales data (10,000 records for good performance)
    sales_data = []
    base_date = datetime(2024, 1, 1)
    
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
    regions = ['North', 'South', 'East', 'West']
    segments = ['Enterprise', 'SMB', 'Consumer']
    
    for i in range(10000):
        transaction_date = base_date + timedelta(days=random.randint(0, 365))
        quantity = random.randint(1, 5)
        unit_price = random.uniform(50, 2000)
        
        sales_data.append({
            'transaction_id': f'TXN_{i+1:06d}',
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'product': random.choice(products),
            'region': random.choice(regions),
            'sales_rep': f'Rep_{random.randint(1, 20):02d}',
            'quantity': quantity,
            'unit_price': builtins.round(unit_price, 2),  # Use Python's round
            'total_amount': builtins.round(quantity * unit_price, 2),
            'customer_segment': random.choice(segments)
        })
    
    # Create customer data
    customer_data = []
    for i in range(1000):
        customer_data.append({
            'customer_id': f'CUST_{i+1:05d}',
            'company_name': f'Company_{i+1}',
            'region': random.choice(regions),
            'industry': random.choice(['Tech', 'Healthcare', 'Finance', 'Retail']),
            'annual_revenue': random.randint(100000, 10000000)
        })
    
    # Create product catalog
    product_catalog = [
        {'product': 'Laptop', 'category': 'Computing', 'cost': 800, 'margin': 0.3},
        {'product': 'Mouse', 'category': 'Accessories', 'cost': 20, 'margin': 0.5},
        {'product': 'Keyboard', 'category': 'Accessories', 'cost': 50, 'margin': 0.4},
        {'product': 'Monitor', 'category': 'Display', 'cost': 300, 'margin': 0.35},
        {'product': 'Headphones', 'category': 'Audio', 'cost': 100, 'margin': 0.45}
    ]
    
    # Save to CSV
    pd.DataFrame(sales_data).to_csv('workshop_sales.csv', index=False)
    pd.DataFrame(customer_data).to_csv('workshop_customers.csv', index=False)
    pd.DataFrame(product_catalog).to_csv('workshop_products.csv', index=False)
    
    print("✅ Created datasets:")
    print(f"  • workshop_sales.csv: {len(sales_data):,} records")
    print(f"  • workshop_customers.csv: {len(customer_data):,} records") 
    print(f"  • workshop_products.csv: {len(product_catalog)} records")

# ==========================================
# EXERCISE 1: BASIC OPERATIONS
# ==========================================

def exercise_1_basic_spark_operations():
    """Basic Spark operations without conflicts"""
    print("\n" + "="*60)
    print("EXERCISE 1: BASIC SPARK OPERATIONS")
    print("="*60)
    
    spark = create_workshop_spark()
    
    try:
        # Create simple test data directly in Spark
        print("📊 Creating test data...")
        people_data = [
            (1, "Alice Johnson", 28, "Engineering", "alice@company.com"),
            (2, "Bob Smith", 35, "Marketing", "bob@company.com"),
            (3, "Carol Davis", 42, "Engineering", "carol@company.com"),
            (4, "David Wilson", 31, "Sales", "david@company.com"),
            (5, "Eve Brown", 26, "Marketing", "eve@company.com"),
            (6, "Frank Miller", 45, "Engineering", "frank@company.com")
        ]
        
        # Create DataFrame directly (no file I/O issues)
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("department", StringType(), False),
            StructField("email", StringType(), False)
        ])
        
        people_df = spark.createDataFrame(people_data, schema)
        
        print("📋 Original data:")
        people_df.show()
        
        # Filter by age > 30
        print("\n🔍 Filtering: age > 30")
        filtered_df = people_df.filter(F.col("age") > 30)
        filtered_df.show()
        
        # Add calculated columns
        print("\n➕ Adding calculated columns:")
        enhanced_df = filtered_df \
            .withColumn("age_doubled", F.col("age") * 2) \
            .withColumn("age_category",
                       F.when(F.col("age") < 35, "Young Professional")
                       .when(F.col("age") < 45, "Mid-Career")
                       .otherwise("Senior Professional")) \
            .withColumn("email_domain", 
                       F.regexp_extract(F.col("email"), "@(.+)", 1))
        
        enhanced_df.show()
        
        # Save results
        print("\n💾 Saving results...")
        enhanced_df.coalesce(1).write.mode("overwrite").csv("output/exercise1", header=True)
        
        print("✅ Exercise 1 completed successfully!")
        
        return enhanced_df
        
    finally:
        spark.stop()

# ==========================================
# EXERCISE 2: JOINS AND AGGREGATIONS
# ==========================================

def exercise_2_joins_and_aggregations():
    """Advanced joins and aggregations"""
    print("\n" + "="*60)
    print("EXERCISE 2: JOINS AND AGGREGATIONS")
    print("="*60)
    
    spark = create_workshop_spark()
    
    try:
        # Create employee data
        employees_data = [
            (1, "Alice", 1, 85000, "2020-01-15"),
            (2, "Bob", 2, 75000, "2019-03-20"),
            (3, "Carol", 1, 95000, "2018-07-10"),
            (4, "David", 3, 65000, "2021-02-28"),
            (5, "Eve", 2, 70000, "2020-11-05"),
            (6, "Frank", 1, 105000, "2017-09-12"),
            (7, "Grace", 4, 80000, "2019-06-18"),
            (8, "Henry", 3, 68000, "2021-08-03")
        ]
        
        # Create department data
        dept_data = [
            (1, "Engineering", "Technology"),
            (2, "Marketing", "Business"),
            (3, "Sales", "Business"),
            (4, "HR", "Operations")
        ]
        
        # Create DataFrames
        employees_df = spark.createDataFrame(employees_data, 
                                           ["emp_id", "name", "dept_id", "salary", "hire_date"])
        dept_df = spark.createDataFrame(dept_data, 
                                      ["dept_id", "dept_name", "division"])
        
        print("👥 Employees:")
        employees_df.show()
        
        print("🏢 Departments:")
        dept_df.show()
        
        # Join operations
        print("\n🔗 Joining data...")
        joined_df = employees_df.join(dept_df, "dept_id", "inner")
        joined_df.show()
        
        # Aggregations by department
        print("\n📊 Department aggregations:")
        dept_summary = joined_df \
            .groupBy("dept_name", "division") \
            .agg(
                F.sum("salary").alias("total_salary"),
                F.avg("salary").alias("avg_salary"),
                F.count("emp_id").alias("employee_count"),
                F.max("salary").alias("max_salary"),
                F.min("salary").alias("min_salary")
            ) \
            .withColumn("avg_salary_rounded", F.round(F.col("avg_salary"), 2)) \
            .orderBy(F.desc("total_salary"))
        
        dept_summary.show()
        
        # Window functions - ranking employees within departments
        print("\n🏆 Employee rankings within departments:")
        window_spec = Window.partitionBy("dept_name").orderBy(F.desc("salary"))
        
        ranked_employees = joined_df \
            .withColumn("salary_rank", F.row_number().over(window_spec)) \
            .withColumn("salary_percentile", F.percent_rank().over(window_spec)) \
            .select("name", "dept_name", "salary", "salary_rank", "salary_percentile") \
            .orderBy("dept_name", "salary_rank")
        
        ranked_employees.show()
        
        # Save results
        print("\n💾 Saving results...")
        dept_summary.coalesce(1).write.mode("overwrite").csv("output/dept_summary", header=True)
        ranked_employees.coalesce(1).write.mode("overwrite").csv("output/employee_rankings", header=True)
        
        print("✅ Exercise 2 completed successfully!")
        
        return joined_df, dept_summary
        
    finally:
        spark.stop()

# ==========================================
# EXERCISE 3: WORKING WITH FILES
# ==========================================

def exercise_3_file_operations():
    """Working with CSV files and partitioning"""
    print("\n" + "="*60)
    print("EXERCISE 3: FILE OPERATIONS AND PARTITIONING")
    print("="*60)
    
    # First create the datasets
    create_workshop_datasets()
    
    spark = create_workshop_spark()
    
    try:
        # Define explicit schemas to avoid type conflicts
        sales_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("product", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sales_rep", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("customer_segment", StringType(), True)
        ])
        
        customers_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("annual_revenue", LongType(), True)
        ])
        
        products_schema = StructType([
            StructField("product", StringType(), True),
            StructField("category", StringType(), True),
            StructField("cost", DoubleType(), True),
            StructField("margin", DoubleType(), True)
        ])
        
        # Read CSV files with explicit schemas
        print("📖 Reading CSV files with explicit schemas...")
        sales_df = spark.read.csv("workshop_sales.csv", header=True, schema=sales_schema)
        customers_df = spark.read.csv("workshop_customers.csv", header=True, schema=customers_schema)
        products_df = spark.read.csv("workshop_products.csv", header=True, schema=products_schema)
        
        print(f"📊 Sales records: {sales_df.count():,}")
        print("Sample sales data:")
        sales_df.limit(5).show()
        
        # Data transformations
        print("\n🔄 Data transformations...")
        enriched_sales = sales_df \
            .withColumn("year", F.year(F.to_date(F.col("transaction_date")))) \
            .withColumn("month", F.month(F.to_date(F.col("transaction_date")))) \
            .withColumn("revenue_category",
                       F.when(F.col("total_amount") < 500, "Small")
                       .when(F.col("total_amount") < 2000, "Medium")
                       .otherwise("Large"))
        
        # Join with products for cost analysis
        print("\n🔗 Joining with product catalog...")
        sales_with_products = enriched_sales \
            .join(products_df, "product", "inner") \
            .withColumn("cost_total", F.col("quantity").cast("double") * F.col("cost")) \
            .withColumn("profit", F.col("total_amount") - F.col("cost_total"))
        
        sales_with_products.cache()  # Cache for multiple operations
        
        # Aggregations
        print("\n📊 Monthly sales analysis:")
        monthly_sales = sales_with_products \
            .groupBy("year", "month", "region") \
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.sum("profit").alias("total_profit"),
                F.count("transaction_id").alias("transaction_count"),
                F.avg("total_amount").alias("avg_transaction_value")
            ) \
            .orderBy("year", "month", F.desc("total_revenue"))
        
        monthly_sales.show(20)
        
        # Product performance
        print("\n🏆 Product performance:")
        product_performance = sales_with_products \
            .groupBy("product", "category") \
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.sum("profit").alias("total_profit"),
                F.sum("quantity").alias("total_quantity"),
                F.avg("profit").alias("avg_profit_per_sale")
            ) \
            .withColumn("profit_margin", F.col("total_profit") / F.col("total_revenue")) \
            .orderBy(F.desc("total_revenue"))
        
        product_performance.show()
        
        # Partitioning demonstration (safe version)
        print("\n📂 Partitioning analysis:")
        print(f"Original partitions: {sales_with_products.rdd.getNumPartitions()}")
        
        # Repartition by region for better performance
        partitioned_by_region = sales_with_products.repartition(F.col("region"))
        print(f"After repartitioning by region: {partitioned_by_region.rdd.getNumPartitions()}")
        
        # Save partitioned data
        print("\n💾 Saving partitioned data...")
        partitioned_by_region.write \
            .partitionBy("region", "year") \
            .mode("overwrite") \
            .parquet("output/sales_partitioned")
        
        # Save summary reports
        monthly_sales.coalesce(1).write.mode("overwrite").csv("output/monthly_sales", header=True)
        product_performance.coalesce(1).write.mode("overwrite").csv("output/product_performance", header=True)
        
        print("✅ Exercise 3 completed successfully!")
        
        return sales_with_products, monthly_sales, product_performance
        
    finally:
        spark.stop()

# ==========================================
# EXERCISE 4: DATA QUALITY CHECKS
# ==========================================

def exercise_4_data_quality():
    """Data quality monitoring and validation"""
    print("\n" + "="*60)
    print("EXERCISE 4: DATA QUALITY MONITORING")
    print("="*60)
    
    spark = create_workshop_spark()
    
    try:
        # Define schema for consistent reading
        sales_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("product", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sales_rep", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("customer_segment", StringType(), True)
        ])
        
        # Read the sales data with explicit schema
        sales_df = spark.read.csv("workshop_sales.csv", header=True, schema=sales_schema)
        
        total_records = sales_df.count()
        print(f"📊 Total records: {total_records:,}")
        
        # 1. Completeness checks
        print("\n🔍 Completeness Analysis:")
        for column in sales_df.columns:
            null_count = sales_df.filter(F.col(column).isNull()).count()
            completeness_rate = ((total_records - null_count) / total_records) * 100
            print(f"  • {column}: {completeness_rate:.1f}% complete ({null_count} nulls)")
        
        # 2. Data validation
        print("\n✅ Data Validation:")
        
        # Check for negative amounts
        negative_amounts = sales_df.filter(F.col("total_amount") < 0).count()
        print(f"  • Negative amounts: {negative_amounts} records")
        
        # Check for invalid quantities
        invalid_quantities = sales_df.filter(F.col("quantity") <= 0).count()
        print(f"  • Invalid quantities: {invalid_quantities} records")
        
        # Check amount calculation consistency
        validation_df = sales_df \
            .withColumn("calculated_amount", F.col("quantity") * F.col("unit_price")) \
            .withColumn("amount_diff", F.abs(F.col("total_amount") - F.col("calculated_amount")))
        
        inconsistent_amounts = validation_df.filter(F.col("amount_diff") > 0.01).count()
        print(f"  • Amount calculation inconsistencies: {inconsistent_amounts} records")
        
        # 3. Outlier detection using percentiles
        print("\n📈 Outlier Detection:")
        
        # Calculate percentiles
        percentiles = sales_df.select(
            F.expr("percentile_approx(total_amount, 0.25)").alias("q1"),
            F.expr("percentile_approx(total_amount, 0.75)").alias("q3"),
            F.avg("total_amount").alias("mean"),
            F.stddev("total_amount").alias("stddev")
        ).collect()[0]
        
        # IQR method for outliers
        iqr = percentiles["q3"] - percentiles["q1"]
        lower_bound = percentiles["q1"] - 1.5 * iqr
        upper_bound = percentiles["q3"] + 1.5 * iqr
        
        outliers = sales_df.filter(
            (F.col("total_amount") < lower_bound) | 
            (F.col("total_amount") > upper_bound)
        ).count()
        
        print(f"  • Statistical outliers (IQR): {outliers} records")
        print(f"  • Normal range: ${lower_bound:.2f} - ${upper_bound:.2f}")
        
        # 4. Duplicate detection
        print("\n🔄 Duplicate Analysis:")
        
        duplicate_ids = sales_df \
            .groupBy("transaction_id") \
            .count() \
            .filter(F.col("count") > 1) \
            .count()
        
        print(f"  • Duplicate transaction IDs: {duplicate_ids}")
        
        # 5. Data quality summary
        print("\n🎯 Data Quality Summary:")
        validity_rate = ((total_records - negative_amounts - invalid_quantities - inconsistent_amounts) / total_records) * 100
        uniqueness_rate = ((total_records - duplicate_ids) / total_records) * 100
        
        print(f"  • Validity Score: {validity_rate:.1f}%")
        print(f"  • Uniqueness Score: {uniqueness_rate:.1f}%")
        print(f"  • Outlier Rate: {(outliers/total_records)*100:.1f}%")
        
        # Create quality report
        quality_metrics = [
            ("validity_rate", f"{validity_rate:.2f}%"),
            ("uniqueness_rate", f"{uniqueness_rate:.2f}%"),
            ("outlier_rate", f"{(outliers/total_records)*100:.2f}%"),
            ("total_records", str(total_records))
        ]
        
        quality_df = spark.createDataFrame(quality_metrics, ["metric", "value"])
        quality_df.show()
        
        # Save quality report
        quality_df.coalesce(1).write.mode("overwrite").csv("output/data_quality_report", header=True)
        
        print("✅ Exercise 4 completed successfully!")
        
        return quality_df
        
    finally:
        # spark.stop()
        pass

# ==========================================
# MAIN WORKSHOP RUNNER
# ==========================================

def run_complete_workshop():
    """Run the complete workshop with all exercises"""
    print("🚀 COMPLETE PYSPARK WORKSHOP")
    print("=" * 60)
    print("🎯 All function conflicts resolved!")
    print("🐳 Running in Docker environment")
    print("=" * 60)
    
    try:
        # Exercise 1: Basic operations
        print("\n🎯 Starting Exercise 1...")
        exercise_1_basic_spark_operations()
        
        # Exercise 2: Joins and aggregations  
        print("\n🎯 Starting Exercise 2...")
        exercise_2_joins_and_aggregations()
        
        # Exercise 3: File operations
        print("\n🎯 Starting Exercise 3...")
        exercise_3_file_operations()
        
        # Exercise 4: Data quality
        print("\n🎯 Starting Exercise 4...")
        exercise_4_data_quality()
        
        print("\n" + "="*60)
        print("🎉 WORKSHOP COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        print("\n📁 Files created:")
        print("  • output/exercise1/ - Basic operations results")
        print("  • output/dept_summary/ - Department aggregations")
        print("  • output/employee_rankings/ - Employee rankings")
        print("  • output/monthly_sales/ - Monthly sales analysis")
        print("  • output/product_performance/ - Product performance")
        print("  • output/sales_partitioned/ - Partitioned data (Parquet)")
        print("  • output/data_quality_report/ - Data quality metrics")
        
        print("\n🎓 Concepts covered:")
        print("  ✅ DataFrames and RDDs")
        print("  ✅ Transformations vs Actions")
        print("  ✅ Joins and Aggregations")
        print("  ✅ Window Functions")
        print("  ✅ Data Partitioning")
        print("  ✅ Data Quality Monitoring")
        print("  ✅ File I/O (CSV, Parquet)")
        print("  ✅ Performance Optimization")
        
        print("\n🌐 While exercises were running, Spark UI was available at:")
        print("    http://localhost:4040")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        print("Check the error message above for details.")

# ==========================================
# SIMPLE DEMO FOR TESTING
# ==========================================

def simple_demo():
    """Simple demo to test everything works"""
    print("🧪 SIMPLE DEMO - Testing Setup")
    print("=" * 40)
    
    spark = create_workshop_spark()
    
    try:
        # Create simple test data
        data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Carol", 35)]
        df = spark.createDataFrame(data, ["id", "name", "age"])
        
        print("📊 Test data:")
        df.show()
        
        # Simple operations
        result = df.filter(F.col("age") > 28).withColumn("age_plus_10", F.col("age") + 10)
        
        print("🔍 Filtered and enhanced:")
        result.show()
        
        print("✅ Demo successful! Spark is working correctly.")
        print("🌐 Check Spark UI at: http://localhost:4040")
        
    finally:
        # spark.stop()
        pass

# ==========================================
# ENTRY POINT
# ==========================================

if __name__ == "__main__":
    print("Choose an option:")
    print("1. Run simple demo (recommended first)")
    print("2. Run complete workshop")
    
    # For automatic execution, just run the complete workshop
    run_complete_workshop()
    
    # Uncomment this line if you want to run just the simple demo first:
    # simple_demo()