# PySpark Course Syllabus: Customer Data Analysis

## 1. Introduction to PySpark and Setup
- What is PySpark?
- Setting up a SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()
```

## 2. Loading and Inspecting Data
- Reading different file formats (CSV, Parquet)
- Examining the schema and data

Example 1: Reading CSV with inferred schema
```python
df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer_data.csv")
df.printSchema()
df.show(5)
```

Example 2: Reading CSV with custom schema
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("ID", IntegerType(), False),
    StructField("Year_Birth", IntegerType(), True),
    StructField("Education", StringType(), True),
    StructField("Marital_Status", StringType(), True),
    StructField("Income", DoubleType(), True),
    # ... add all other fields
])

df = spark.read.option("header", "true").schema(schema).csv("customer_data.csv")
df.printSchema()
```

## 3. Basic Column Operations
- Selecting columns
- Renaming columns
- Adding new columns

Example 1: Selecting and renaming columns
```python
from pyspark.sql.functions import col

df_selected = df.select(
    "ID",
    "Year_Birth",
    col("Education").alias("EducationLevel"),
    "Income"
)
df_selected.show(5)
```

Example 2: Adding a new column
```python
from pyspark.sql.functions import current_year

df_with_age = df.withColumn("Age", current_year() - df.Year_Birth)
df_with_age.select("ID", "Year_Birth", "Age").show(5)
```

## 4. Filtering and Sorting Data
- Filtering rows based on conditions
- Sorting data

Example 1: Filtering data using `filter` and `where`
```python
high_income = df.filter(df.Income > 50000)
high_income.show(5)

young_customers = df.where((df.Age < 30) & (df.Income > 30000))
young_customers.show(5)
```

Example 2: Sorting data using `orderBy`
```python
sorted_by_income = df.orderBy(df.Income.desc())
sorted_by_income.select("ID", "Income").show(5)

sorted_by_age_and_income = df.orderBy(df.Age.asc(), df.Income.desc())
sorted_by_age_and_income.select("ID", "Age", "Income").show(5)
```

## 5. String Functions and Regular Expressions
- Basic string operations
- Regular expression operations

Example 1: Using `trim`, `lower`, and `concat`
```python
from pyspark.sql.functions import trim, lower, concat, lit

df_cleaned = df.withColumn("Education", trim(lower(df.Education)))
df_cleaned = df_cleaned.withColumn("FullName", concat(df.FirstName, lit(" "), df.LastName))
df_cleaned.select("ID", "Education", "FullName").show(5)
```

Example 2: Using `regexp_replace` for data cleaning
```python
from pyspark.sql.functions import regexp_replace

df_cleaned = df.withColumn("PhoneNumber", 
    regexp_replace(df.PhoneNumber, r"(\d{3})-(\d{3})-(\d{4})", "($1) $2-$3"))
df_cleaned.select("ID", "PhoneNumber").show(5)
```

## 6. Data Type Casting and Handling
- Casting between data types
- Handling date and timestamp data

Example 1: Casting data types
```python
from pyspark.sql.functions import col

df_casted = df.withColumn("Income", col("Income").cast("integer"))
df_casted = df_casted.withColumn("HasChildren", col("Kidhome").cast("boolean"))
df_casted.printSchema()
```

Example 2: Working with date functions
```python
from pyspark.sql.functions import to_date, datediff, current_date

df_dates = df.withColumn("Dt_Customer", to_date(df.Dt_Customer, "dd-MM-yyyy"))
df_dates = df_dates.withColumn("DaysSinceJoined", datediff(current_date(), df_dates.Dt_Customer))
df_dates.select("ID", "Dt_Customer", "DaysSinceJoined").show(5)
```

## 7. Handling Missing Data
- Checking for null values
- Dropping null values
- Filling null values

Example 1: Checking and dropping null values
```python
from pyspark.sql.functions import count, when, col

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

df_no_nulls = df.dropna()
print(f"Rows before: {df.count()}, Rows after: {df_no_nulls.count()}")
```

Example 2: Filling null values
```python
from pyspark.sql.functions import mean

df_filled = df.fillna({
    "Income": df.select(mean("Income")).first()[0],
    "Education": "Unknown"
})
df_filled.filter(col("Education") == "Unknown").show(5)
```

## 8. Aggregations and Grouping
- Basic aggregations (count, sum, average)
- Grouping data
- Multiple aggregations

Example 1: Basic aggregations
```python
from pyspark.sql.functions import count, mean, sum

summary = df.agg(
    count("ID").alias("TotalCustomers"),
    mean("Income").alias("AverageIncome"),
    sum("MntWines").alias("TotalWinesSales")
)
summary.show()
```

Example 2: Grouping and aggregating
```python
education_summary = df.groupBy("Education").agg(
    count("ID").alias("CustomerCount"),
    mean("Income").alias("AverageIncome"),
    sum("MntTotal").alias("TotalSales")
)
education_summary.show()
```

## 9. Join Operations
- Inner joins
- Left outer joins

Example 1: Inner join with a campaign performance dataset
```python
campaign_data = [
    (1, "Email", 0.05),
    (2, "Social Media", 0.03),
    (3, "TV", 0.02),
    (4, "Radio", 0.01),
    (5, "Newspaper", 0.02)
]
campaign_df = spark.createDataFrame(campaign_data, ["CampaignID", "Channel", "ConversionRate"])

df_with_campaign = df.join(campaign_df, df.AcceptedCmp1 == campaign_df.CampaignID, "inner")
df_with_campaign.select("ID", "AcceptedCmp1", "Channel", "ConversionRate").show(5)
```

Example 2: Left outer join with product categories
```python
product_categories = [
    ("Wines", "Alcohol"),
    ("Fruits", "Fresh Produce"),
    ("Meat", "Protein"),
    ("Fish", "Protein"),
    ("Sweets", "Confectionery")
]
category_df = spark.createDataFrame(product_categories, ["ProductType", "Category"])

df_with_categories = df.join(category_df, df.MntWines == category_df.ProductType, "left_outer")
df_with_categories.select("ID", "MntWines", "ProductType", "Category").show(5)
```

## 10. User-Defined Functions (UDFs)
- Creating UDFs
- Applying UDFs to DataFrames

Example 1: UDF for customer segmentation
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def customer_segment(age, income):
    if age < 35:
        return "Young Budget" if income < 50000 else "Young Affluent"
    else:
        return "Mature Budget" if income < 50000 else "Mature Affluent"

segment_udf = udf(customer_segment, StringType())

df_with_segment = df.withColumn("CustomerSegment", segment_udf(df.Age, df.Income))
df_with_segment.select("ID", "Age", "Income", "CustomerSegment").show(10)
```

Example 2: UDF for calculating customer lifetime value
```python
from pyspark.sql.types import DoubleType

def customer_ltv(total_spent, years_as_customer):
    return total_spent / years_as_customer * 5 if years_as_customer > 0 else total_spent

ltv_udf = udf(customer_ltv, DoubleType())

df_with_ltv = df.withColumn("YearsAsCustomer", (current_date() - to_date(df.Dt_Customer, "dd-MM-yyyy")) / 365) \
    .withColumn("LTV", ltv_udf(df.MntTotal, "YearsAsCustomer"))

df_with_ltv.select("ID", "MntTotal", "YearsAsCustomer", "LTV").show(10)
```

## 11. Spark SQL
- Creating temporary views
- Running SQL queries on DataFrames

Example 1: Basic Spark SQL query
```python
df.createOrReplaceTempView("customers")

sql_result = spark.sql("""
    SELECT Education, AVG(Income) as AvgIncome, COUNT(*) as CustomerCount
    FROM customers
    GROUP BY Education
    ORDER BY AvgIncome DESC
""")
sql_result.show()
```

Example 2: Complex Spark SQL query with subquery
```python
spark.sql("""
    WITH customer_segments AS (
        SELECT *,
            CASE
                WHEN Age < 35 AND Income < 50000 THEN 'Young Budget'
                WHEN Age < 35 AND Income >= 50000 THEN 'Young Affluent'
                WHEN Age >= 35 AND Income < 50000 THEN 'Mature Budget'
                ELSE 'Mature Affluent'
            END AS CustomerSegment
        FROM customers
    )
    SELECT CustomerSegment, 
           AVG(MntTotal) as AvgTotalSpent,
           AVG(NumWebVisitsMonth) as AvgWebVisits
    FROM customer_segments
    GROUP BY CustomerSegment
    ORDER BY AvgTotalSpent DESC
""").show()
```

## 12. Advanced String Manipulation
- Substring extraction
- Levenshtein distance for fuzzy matching

Example 1: Using `substring` function
```python
from pyspark.sql.functions import substring

df_substring = df.withColumn("BirthYear", substring(df.Dt_Customer, -4, 4))
df_substring.select("ID", "Dt_Customer", "BirthYear").show(5)
```

Example 2: Using Levenshtein distance for product name matching
```python
from pyspark.sql.functions import levenshtein

product_names = spark.createDataFrame([
    ("Cabernet Sauvignon",),
    ("Chardonnay",),
    ("Merlot",),
    ("Pinot Noir",)
], ["StandardName"])

df_with_fuzzy_match = df.withColumn("ProductName", df.ProductName.cast("string"))
df_with_fuzzy_match = df_with_fuzzy_match.crossJoin(product_names) \
    .withColumn("LevenshteinDistance", levenshtein(df.ProductName, product_names.StandardName))

df_with_fuzzy_match.groupBy("ProductName") \
    .agg({"LevenshteinDistance": "min"}) \
    .join(df_with_fuzzy_match, ["ProductName", "LevenshteinDistance"]) \
    .select("ProductName", "StandardName", "LevenshteinDistance") \
    .orderBy("LevenshteinDistance") \
    .show()
```

## 13. Data Cleaning and Preprocessing
- Handling outliers
- Standardizing categorical variables

Example 1: Detecting and handling outliers using Z-score
```python
from pyspark.sql.functions import col, mean, stddev

df_stats = df.select(mean("Income").alias("mean"), stddev("Income").alias("stddev"))
df_with_zscore = df.withColumn("ZScore", (col("Income") - df_stats.select("mean").first()[0]) / df_stats.select("stddev").first()[0])

df_without_outliers = df_with_zscore.filter(abs(col("ZScore")) < 3)
print(f"Rows before: {df.count()}, Rows after: {df_without_outliers.count()}")
```

Example 2: Standardizing education levels
```python
from pyspark.sql.functions import when

df_standardized = df.withColumn("EducationStd", 
    when(col("Education").isin("2n Cycle", "Graduate", "Master", "PhD"), "Higher Education")
    .when(col("Education") == "Basic", "Basic")
    .otherwise("Unknown")
)
df_standardized.groupBy("EducationStd").count().show()
```

## 14. Writing and Saving Data
- Writing to CSV
- Writing to Parquet
- Partitioning data

Example 1: Writing to CSV
```python
df_with_segment.write.mode("overwrite").option("header", "true").csv("customer_segments.csv")
```

Example 2: Writing to partitioned Parquet
```python
df_with_ltv.write.partitionBy("CustomerSegment").mode("overwrite").parquet("customer_ltv.parquet")
```

## 15. Performance Optimization
- Caching
- Persistence levels
- Query optimization

Example 1: Caching a DataFrame
```python
df.cache()
df.count()  # Action to materialize the cache
```

Example 2: Setting persistence level
```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # Action to materialize the persisted data
```

## 16. Best Practices and Conclusion
- Code organization
- Error handling
- Performance considerations
- Real-world applications of PySpark in data engineering