# PySpark-Notes

## 1. PySpark Basics
- Introduction to PySpark
- Setting up SparkSession

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()
```

## 2. Reading Data
### 2.1 Reading CSV with infer schema
```python
df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer_data.csv")
```

### 2.2 Reading CSV with custom schema
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Year_Birth", IntegerType(), True),
    StructField("Education", StringType(), True),
    StructField("Marital_Status", StringType(), True),
    # ... add all other fields
])

df = spark.read.option("header", "true").schema(schema).csv("customer_data.csv")
```

### 2.3 Setting custom delimiter
```python
df = spark.read.option("header", "true").option("delimiter", "|").csv("customer_data.csv")
```

## 3. Data Exploration
- Viewing the schema: `df.printSchema()`
- Displaying data: `df.show()`, `df.head()`, `df.take(n)`
- Describing data: `df.describe().show()`

## 4. Data Transformation
### 4.1 Selecting columns
```python
df.select("ID", "Education", "Income").show()
```

### 4.2 Creating new columns
```python
from pyspark.sql.functions import col, year, current_date

df = df.withColumn("Age", year(current_date()) - col("Year_Birth"))
df = df.withColumn("TotalPurchases", col("NumWebPurchases") + col("NumCatalogPurchases") + col("NumStorePurchases"))
```

### 4.3 Renaming columns
```python
df = df.withColumnRenamed("Dt_Customer", "CustomerSince")
```

### 4.4 Casting column types
```python
from pyspark.sql.functions import to_date
df = df.withColumn("Dt_Customer", to_date(col("Dt_Customer"), "dd-MM-yyyy"))
```

## 5. Data Filtering and Sorting
### 5.1 Filtering data
```python
high_income = df.filter(col("Income") > 50000)
recent_customers = df.filter(col("Recency") < 30)
```

### 5.2 Sorting data
```python
df_sorted = df.orderBy(col("Income").desc())
```

## 6. Aggregations and Grouping
### 6.1 GroupBy operations
```python
from pyspark.sql.functions import avg, sum, count

education_summary = df.groupBy("Education").agg(
    avg("Income").alias("AvgIncome"),
    count("ID").alias("CustomerCount")
)
```

### 6.2 Window functions
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("Education").orderBy(col("Income").desc())
df_with_rank = df.withColumn("IncomeRank", row_number().over(windowSpec))
```

## 7. Joins
```python
# Assuming we have another DataFrame 'product_df'
joined_df = df.join(product_df, "ProductID", "left")
```

## 8. Handling Missing Data
```python
df_no_nulls = df.na.drop()
df_filled = df.na.fill({"Income": 0, "Education": "Unknown"})
```

## 9. User-Defined Functions (UDFs)
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def income_category(income):
    if income < 30000:
        return "Low"
    elif income < 70000:
        return "Medium"
    else:
        return "High"

income_category_udf = udf(income_category, StringType())
df = df.withColumn("IncomeCategory", income_category_udf(col("Income")))
```

## 10. Partitioning and Bucketing
### 10.1 Writing partitioned data
```python
df.write.partitionBy("Education").parquet("customer_data.parquet")
```

### 10.2 Reading partitioned data
```python
df_graduate = spark.read.parquet("customer_data.parquet/Education=Graduation")
```

## 11. Performance Optimization
- Caching: `df.cache()`
- Persistence: `df.persist(StorageLevel.MEMORY_AND_DISK)`
- Unpersist: `df.unpersist()`

## 12. Spark SQL
```python
df.createOrReplaceTempView("customers")
result = spark.sql("SELECT Education, AVG(Income) as AvgIncome FROM customers GROUP BY Education")
```

## 13. Data Quality Checks
```python
from pyspark.sql.functions import count, when, isnan, col

def check_data_quality(df):
    return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

quality_check = check_data_quality(df)
```

## 14. Saving Data
### 14.1 Writing to CSV
```python
df.write.mode("overwrite").option("header", "true").csv("output.csv")
```

### 14.2 Writing to Parquet
```python
df.write.mode("overwrite").parquet("output.parquet")
```

## 15. Spark Streaming (Basic Concept)
- Introduction to structured streaming

## 16. Best Practices
- Optimizing Spark jobs
- Managing resources
- Debugging and monitoring Spark applications
