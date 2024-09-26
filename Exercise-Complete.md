# PySpark Practice Questions for Customer Data Analysis

## Setup
First, let's assume we have our DataFrame loaded:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()
df = spark.read.csv("customer_data.csv", header=True, inferSchema=True)
```

## Questions

1. How many customers are in the dataset?
   
   Tip: Use the `count()` function.

   Solution:
   ```python
   customer_count = df.count()
   print(f"Total number of customers: {customer_count}")
   ```
   Explanation: This simple operation counts all rows in the DataFrame, giving us the total number of customers.

2. What is the average age of the customers?
   
   Tip: Create an 'Age' column first, then calculate the average.

   Solution:
   ```python
   from pyspark.sql.functions import year, current_date

   df_with_age = df.withColumn("Age", year(current_date()) - col("Year_Birth"))
   avg_age = df_with_age.select(avg("Age")).first()[0]
   print(f"Average age of customers: {avg_age:.2f}")
   ```
   Explanation: We first create an 'Age' column by subtracting the birth year from the current year, then calculate the average of this column.

3. How many customers have accepted the offer in the last campaign?
   
   Tip: Filter the DataFrame based on the 'Response' column and count the results.

   Solution:
   ```python
   accepted_last_campaign = df.filter(col("Response") == 1).count()
   print(f"Number of customers who accepted the last campaign: {accepted_last_campaign}")
   ```
   Explanation: We filter the DataFrame to include only rows where 'Response' is 1, then count these rows.

4. What is the total amount spent on wines by all customers?
   
   Tip: Use the `sum()` function on the 'MntWines' column.

   Solution:
   ```python
   total_wine_spent = df.select(sum("MntWines")).first()[0]
   print(f"Total amount spent on wines: ${total_wine_spent:.2f}")
   ```
   Explanation: We select the 'MntWines' column and sum all its values to get the total amount spent on wines.

5. Create a new column 'TotalSpent' that sums up all 'Mnt' columns, then show the top 5 customers by total spending.
   
   Tip: Use `withColumn()` to create the new column, then sort and show the results.

   Solution:
   ```python
   df_with_total = df.withColumn("TotalSpent", 
       col("MntWines") + col("MntFruits") + col("MntMeatProducts") + 
       col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")
   )
   df_with_total.orderBy(col("TotalSpent").desc()).select("ID", "TotalSpent").show(5)
   ```
   Explanation: We create a 'TotalSpent' column by summing all 'Mnt' columns, then sort the DataFrame by this column in descending order and show the top 5 rows.

6. What is the most common education level among the customers?
   
   Tip: Use `groupBy()` and `count()`, then sort to find the most common.

   Solution:
   ```python
   most_common_education = df.groupBy("Education").count().orderBy(col("count").desc()).first()
   print(f"Most common education level: {most_common_education['Education']} (Count: {most_common_education['count']})")
   ```
   Explanation: We group the DataFrame by the 'Education' column, count the occurrences, sort in descending order, and take the first result.

7. Calculate the average income for customers with and without children.
   
   Tip: Create a boolean column for having children, then use `groupBy()` and `avg()`.

   Solution:
   ```python
   df_with_children = df.withColumn("HasChildren", (col("Kidhome") + col("Teenhome") > 0))
   result = df_with_children.groupBy("HasChildren").agg(avg("Income").alias("AvgIncome"))
   result.show()
   ```
   Explanation: We create a boolean column 'HasChildren' based on 'Kidhome' and 'Teenhome', then group by this column and calculate the average income for each group.

8. How many customers have complained in the last 2 years?
   
   Tip: Filter the DataFrame based on the 'Complain' column and count the results.

   Solution:
   ```python
   complain_count = df.filter(col("Complain") == 1).count()
   print(f"Number of customers who complained: {complain_count}")
   ```
   Explanation: We filter the DataFrame to include only rows where 'Complain' is 1, then count these rows.

9. What is the average number of days since the last purchase (Recency) for each marital status?
   
   Tip: Use `groupBy()` on 'Marital_Status' and calculate the average of 'Recency'.

   Solution:
   ```python
   avg_recency_by_marital = df.groupBy("Marital_Status").agg(avg("Recency").alias("AvgRecency"))
   avg_recency_by_marital.show()
   ```
   Explanation: We group the DataFrame by 'Marital_Status' and calculate the average of the 'Recency' column for each group.

10. Create a new column 'AgeGroup' categorizing customers as 'Young' (< 35), 'Middle-aged' (35-55), or 'Senior' (> 55).
    
    Tip: Use `when()` and `otherwise()` functions to create the new column.

    Solution:
    ```python
    df_with_age_group = df.withColumn("Age", year(current_date()) - col("Year_Birth")) \
        .withColumn("AgeGroup", 
            when(col("Age") < 35, "Young")
            .when((col("Age") >= 35) & (col("Age") <= 55), "Middle-aged")
            .otherwise("Senior")
        )
    df_with_age_group.groupBy("AgeGroup").count().show()
    ```
    Explanation: We first calculate the 'Age', then use conditional statements to assign age groups. Finally, we group by the new 'AgeGroup' column and count to see the distribution.

11. Calculate the ratio of total amount spent on wines to the total amount spent on all products for each customer.
    
    Tip: Create a 'TotalSpent' column, then calculate the ratio using 'MntWines'.

    Solution:
    ```python
    df_with_ratio = df.withColumn("TotalSpent", 
        col("MntWines") + col("MntFruits") + col("MntMeatProducts") + 
        col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")
    ).withColumn("WineRatio", col("MntWines") / col("TotalSpent"))
    df_with_ratio.select("ID", "WineRatio").show(5)
    ```
    Explanation: We create a 'TotalSpent' column, then divide 'MntWines' by 'TotalSpent' to get the ratio of spending on wines.

12. Find the top 5 customers who have made the most purchases (sum of all 'Num' columns).
    
    Tip: Create a 'TotalPurchases' column, then sort and show the top 5.

    Solution:
    ```python
    df_with_total_purchases = df.withColumn("TotalPurchases", 
        col("NumDealsPurchases") + col("NumWebPurchases") + 
        col("NumCatalogPurchases") + col("NumStorePurchases")
    )
    df_with_total_purchases.orderBy(col("TotalPurchases").desc()).select("ID", "TotalPurchases").show(5)
    ```
    Explanation: We create a 'TotalPurchases' column by summing all 'Num' columns, then sort by this column in descending order and show the top 5 rows.

13. Calculate the percentage of customers who have accepted at least one campaign offer.
    
    Tip: Create a boolean column for accepting any campaign, then calculate the percentage.

    Solution:
    ```python
    df_with_any_campaign = df.withColumn("AcceptedAnyCampaign", 
        (col("AcceptedCmp1") + col("AcceptedCmp2") + col("AcceptedCmp3") + 
         col("AcceptedCmp4") + col("AcceptedCmp5") + col("Response")) > 0
    )
    percentage = df_with_any_campaign.filter(col("AcceptedAnyCampaign")).count() / df.count() * 100
    print(f"Percentage of customers who accepted at least one campaign: {percentage:.2f}%")
    ```
    Explanation: We create a boolean column 'AcceptedAnyCampaign', then calculate the percentage of True values in this column.

14. Find the average income for customers who have and haven't complained.
    
    Tip: Use `groupBy()` on the 'Complain' column and calculate the average income.

    Solution:
    ```python
    avg_income_by_complaint = df.groupBy("Complain").agg(avg("Income").alias("AvgIncome"))
    avg_income_by_complaint.show()
    ```
    Explanation: We group the DataFrame by the 'Complain' column and calculate the average of the 'Income' column for each group.

15. Create a new column 'EnrollmentYear' extracted from 'Dt_Customer', then find the number of customers enrolled each year.
    
    Tip: Use `year()` function to extract the year, then use `groupBy()` and `count()`.

    Solution:
    ```python
    df_with_enrollment_year = df.withColumn("EnrollmentYear", year(to_date(col("Dt_Customer"), "dd-MM-yyyy")))
    enrollment_by_year = df_with_enrollment_year.groupBy("EnrollmentYear").count().orderBy("EnrollmentYear")
    enrollment_by_year.show()
    ```
    Explanation: We extract the year from 'Dt_Customer' to create 'EnrollmentYear', then group by this new column and count the occurrences for each year.

16. Calculate the correlation between 'Income' and 'TotalSpent'.
    
    Tip: Create a 'TotalSpent' column, then use the `corr()` function.

    Solution:
    ```python
    df_with_total = df.withColumn("TotalSpent", 
        col("MntWines") + col("MntFruits") + col("MntMeatProducts") + 
        col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")
    )
    correlation = df_with_total.select(corr("Income", "TotalSpent")).first()[0]
    print(f"Correlation between Income and TotalSpent: {correlation:.4f}")
    ```
    Explanation: We create a 'TotalSpent' column, then use the `corr()` function to calculate the correlation between 'Income' and 'TotalSpent'.

17. Find the most common combination of Education and Marital_Status.
    
    Tip: Use `groupBy()` on both columns, count, and sort.

    Solution:
    ```python
    common_edu_marital = df.groupBy("Education", "Marital_Status").count().orderBy(col("count").desc())
    common_edu_marital.show(1)
    ```
    Explanation: We group the DataFrame by both 'Education' and 'Marital_Status', count the occurrences, and sort in descending order to find the most common combination.

18. Calculate the average number of web visits per month for customers with different numbers of children.
    
    Tip: Create a 'TotalChildren' column, then use `groupBy()` and average.

    Solution:
    ```python
    df_with_children = df.withColumn("TotalChildren", col("Kidhome") + col("Teenhome"))
    avg_visits_by_children = df_with_children.groupBy("TotalChildren").agg(avg("NumWebVisitsMonth").alias("AvgWebVisits"))
    avg_visits_by_children.orderBy("TotalChildren").show()
    ```
    Explanation: We create a 'TotalChildren' column, then group by this column and calculate the average of 'NumWebVisitsMonth' for each group.

19. Find the top 3 most profitable product categories (excluding wines).
    
    Tip: Sum up the amounts for each product category, then sort and show the top 3.

    Solution:
    ```python
    product_profits = df.select(
        sum("MntFruits").alias("Fruits"),
        sum("MntMeatProducts").alias("Meat"),
        sum("MntFishProducts").alias("Fish"),
        sum("MntSweetProducts").alias("Sweets"),
        sum("MntGoldProds").alias("Gold")
    )
    product_profits_flat = product_profits.select(explode(array([struct(lit(c).alias("category"), col(c).alias("profit")) for c in product_profits.columns])))
    top_products = product_profits_flat.orderBy(col("col.profit").desc()).limit(3)
    top_products.show()
    ```
    Explanation: We sum up the amounts for each product category (excluding wines), then use `explode` to flatten the results, sort by profit in descending order, and show the top 3.

20. Create a new column 'CustomerValue' categorizing customers as 'High', 'Medium', or 'Low' based on their total spending.
    
    Tip: Create a 'TotalSpent' column, then use `when()` and `otherwise()` to categorize.

    Solution:
    ```python
    df_with_total = df.withColumn("TotalSpent", 
        col("MntWines") + col("MntFruits") + col("MntMeatProducts") + 
        col("MntFishProducts") + col("MntSweetProducts") + col("MntGoldProds")
    )
    df_with_value = df_with_total.withColumn("CustomerValue",
        when(col("TotalSpent") > 2000, "High")
        .when((col("TotalSpent") <= 2000) & (col("TotalSpent") > 1000), "Medium")
        .otherwise("Low")
    )
    df_with_value.groupBy("CustomerValue").count().orderBy("CustomerValue").show()
    ```
    Explanation: We create a 'TotalSpent' column, then use conditional statements to categorize customers into 'High', 'Medium', or 'Low' value based on their total spending. Finally, we group by this new category and count to see the distribution.

These questions cover a wide range of PySpark operations and should give your students good practice with data manipulation, analysis, and feature engineering tasks.