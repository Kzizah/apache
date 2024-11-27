import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Employee DataFrame")
  .master("local[*]")
  .getOrCreate()

// 1. Create DataFrame
val data = Seq(
  ("James", "Owino", "Kenya", "Kisumu", 50, 50000),
  ("Michael", "Mungai", "Kenya", "Nakuru", 60, 60000),
  ("Joyce", "Akinyi", "Kenya", "Nyeri", 35, 55000),
  ("Maria", "Jones", "USA", "Florida", 43, 40000),
  ("George", "Okongo", "Kenya", "Nairobi", 16, 70000),
  ("Brian", "Kamau", "Kenya", "Nairobi", 29, 150000),
  ("Melinda", "Williams", "USA", "Georgia", 39, 200000),
  ("James", "Okwamo", "USA", "Florida", 49, 175000),
  ("Meena", "Kimani", "Kenya", "Nakuru", 36, 76000),
  ("Brenda", "Johnson", "USA", "Georgia", 16, 120000),
  ("Johnson", "Mike", "Kenya", "Nairobi", 29, 56000),
  ("Melinda", "Williams", "USA", "Georgia", 19, 45000),
  ("Johnson", "Wambwere", "Kenya", "Nairobi", 29, 79000)
)

val columns = Seq("firstname", "lastname", "country", "City", "Age", "Salary")
val df = spark.createDataFrame(data).toDF(columns: _*)

// 2. Print the column headings
df.printSchema()

// 3. Create and display DataFrame usadf containing only those who work in USA
val usadf = df.filter(col("country") === "USA")
usadf.show()

// 4. Create and display DataFrame df1 containing only firstname, lastname, age, and salary
val df1 = df.select("firstname", "lastname", "Age", "Salary")
df1.show()

// 5. Create and display DataFrame df2 with additional column Trvall (TravelAllowance)
val df2 = df.withColumn("Trvall", 
  when(col("country") === "USA", col("Salary") * 0.20)
  .when(col("country") === "Kenya", col("Salary") * 0.10)
  .otherwise(0)
)
df2.show()

// 6. Create and display DataFrame df3 with additional column Total (Salary + Trvall)
val df3 = df2.withColumn("Total", col("Salary") + col("Trvall"))
df3.show()

// 7. Display df3 grouped by Country
df3.groupBy("country").agg(
  count("*").alias("total_employees"),
  avg("Salary").alias("average_salary"),
  avg("Trvall").alias("average_trvall"),
  avg("Total").alias("average_total")
).show()

// 8. Display sum of Total Earnings grouped by Country
df3.groupBy("country").agg(sum("Total").alias("total_earnings")).show()

// 9. Display first 5 Employees earning the most
df3.orderBy(col("Total").desc).show(5)