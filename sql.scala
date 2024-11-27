import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Employee DataFrame with SQL")
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

// Register the DataFrame as a temporary view
df.createOrReplaceTempView("employees")

// 2. Print the column headings (schema)
df.printSchema()

// 3. Create and display DataFrame usadf containing only those who work in USA
val usadf = spark.sql("SELECT * FROM employees WHERE country = 'USA'")
usadf.show()

// 4. Create and display DataFrame df1 containing only firstname, lastname, age, and salary
val df1 = spark.sql("SELECT firstname, lastname, Age, Salary FROM employees")
df1.show()

// 5. Create and display DataFrame df2 with additional column Trvall (TravelAllowance)
val df2 = spark.sql("""
  SELECT *, 
         CASE 
           WHEN country = 'USA' THEN Salary * 0.20
           WHEN country = 'Kenya' THEN Salary * 0.10
           ELSE 0 
         END AS Trvall
  FROM employees
""")
df2.show()

// 6. Create and display DataFrame df3 with additional column Total (Salary + Trvall)
val df3 = spark.sql("""
  SELECT *, 
         Salary + 
         CASE 
           WHEN country = 'USA' THEN Salary * 0.20
           WHEN country = 'Kenya' THEN Salary * 0.10
           ELSE 0 
         END AS Total
  FROM employees
""")
df3.show()

// 7. Display df3 grouped by Country
spark.sql("""
  SELECT country, 
         COUNT(*) AS total_employees, 
         AVG(Salary) AS average_salary, 
         AVG(CASE 
               WHEN country = 'USA' THEN Salary * 0.20
               WHEN country = 'Kenya' THEN Salary * 0.10
               ELSE 0 
             END) AS average_trvall
  FROM employees 
  GROUP BY country
""").show()

// 8. Display sum of Total Earnings grouped by Country
spark.sql("""
  SELECT country, 
         SUM(Salary + 
             CASE 
               WHEN country = 'USA' THEN Salary * 0.20
               WHEN country = 'Kenya' THEN Salary * 0.10
               ELSE 0 
             END) AS total_earnings
  FROM employees 
  GROUP BY country
""").show()

// 9. Display first 5 Employees earning the most
spark.sql("""
  SELECT *, 
         Salary + 
         CASE 
           WHEN country = 'USA' THEN Salary * 0.20
           WHEN country = 'Kenya' THEN Salary * 0.10
           ELSE 0 
         END AS Total
  FROM employees
  ORDER BY Total DESC
  LIMIT 5
""").show()