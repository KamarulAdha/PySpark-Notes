# PySpark Exercises for Customer Data Analysis

## Setup
First, ensure the data is loaded into a DataFrame:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()

# Assuming the data is in a CSV file named 'customer_data.csv'
df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer_data.csv")
```

## Exercises

1. Data Exploration
   Task: How many unique Education levels are there in the dataset?
   
   Solution:
   ```python
   unique_education = df.select("Education").distinct().count()
   print(f"Number of unique Education levels: {unique_education}")
   ```
   Expected output: The actual number (e.g., 5)

2. Data Transformation
   Task: Create a new column 'TotalChildren' that sums up 'Kidhome' and 'Teenhome'.
   
   Solution:
   ```python
   df_with_total = df.withColumn("TotalChildren", col("Kidhome") + col("Teenhome"))
   df_with_total.select("ID", "Kidhome", "Teenhome", "TotalChildren").show(5)
   ```

3. Filtering
   Task: How many customers have a total amount spent (sum of all 'Mnt' columns) greater than 2000?
   
   Solution:
   ```python
   high_spenders = df.withColumn("TotalSpent", 
       col("MntWines") + col("MntFruits") + col("MntMeatProducts") + 
       col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")
   ).filter(col("TotalSpent") > 2000).count()
   
   print(f"Number of high spenders: {high_spenders}")
   ```

4. Aggregation
   Task: What is the average Income for each Education level?
   
   Solution:
   ```python
   avg_income_by_education = df.groupBy("Education").agg(avg("Income").alias("AvgIncome"))
   avg_income_by_education.show()
   ```

5. Window Functions
   Task: Rank customers within each Education level based on their Income.
   
   Solution:
   ```python
   from pyspark.sql.window import Window
   
   windowSpec = Window.partitionBy("Education").orderBy(col("Income").desc())
   df_with_rank = df.withColumn("IncomeRank", rank().over(windowSpec))
   df_with_rank.select("ID", "Education", "Income", "IncomeRank").show()
   ```

6. Join Operation
   Task: Create a dummy DataFrame with campaign success rates and join it with the main DataFrame.
   
   Solution:
   ```python
   # Create a dummy DataFrame
   campaign_data = [
       (1, 0.2), (2, 0.3), (3, 0.15), (4, 0.25), (5, 0.35)
   ]
   campaign_df = spark.createDataFrame(campaign_data, ["CampaignID", "SuccessRate"])
   
   # Join with main DataFrame
   df_with_campaign = df.join(campaign_df, df.AcceptedCmp1 == campaign_df.CampaignID, "left")
   df_with_campaign.select("ID", "AcceptedCmp1", "SuccessRate").show()
   ```

7. Data Quality Check
   Task: Check for any null values in the dataset and display the count of nulls for each column.
   
   Solution:
   ```python
   null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
   null_counts.show()
   ```

8. Advanced Analysis
   Task: Find the top 3 most common combinations of Education and Marital_Status.
   
   Solution:
   ```python
   common_combinations = df.groupBy("Education", "Marital_Status") \
       .count() \
       .orderBy(col("count").desc()) \
       .limit(3)
   common_combinations.show()
   ```

9. Date Manipulation
   Task: Calculate the number of days since the customer's enrollment date (Dt_Customer) to a reference date (e.g., '2014-12-31').
   
   Solution:
   ```python
   from pyspark.sql.functions import to_date, datediff, lit
   
   reference_date = '2014-12-31'
   df_with_days = df.withColumn("Dt_Customer", to_date(col("Dt_Customer"), "dd-MM-yyyy")) \
       .withColumn("DaysSinceEnrollment", datediff(lit(reference_date), col("Dt_Customer")))
   df_with_days.select("ID", "Dt_Customer", "DaysSinceEnrollment").show()
   ```

10. Partitioning
    Task: Write the DataFrame to parquet format, partitioned by Education and Marital_Status. Then read back data for customers with 'Graduation' education who are 'Married'.
    
    Solution:
    ```python
    # Write partitioned data
    df.write.partitionBy("Education", "Marital_Status").mode("overwrite").parquet("customer_data.parquet")
    
    # Read specific partition
    graduated_married = spark.read.parquet("customer_data.parquet/Education=Graduation/Marital_Status=Married")
    graduated_married.show(5)
    ```