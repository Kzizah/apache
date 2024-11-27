import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("Class Performance")
  .master("local")
  .getOrCreate()

// Define Schema
val schema = StructType(Array(
  StructField("Number", IntegerType, nullable = false),
  StructField("Name", StringType, nullable = false),
  StructField("Gender", StringType, nullable = false),
  StructField("Cat1", IntegerType, nullable = false),
  StructField("Cat2", IntegerType, nullable = false),
  StructField("Exam", IntegerType, nullable = false)
))

// Sample Data
val data = Seq(
  (1, "Alice", "Female", 8, 15, 65),
  (2, "Bob", "Male", 10, 18, 70),
  (3, "Cathy", "Female", 6, 12, 60),
  (4, "David", "Male", 9, 17, 68),
  (5, "Eva", "Female", 7, 16, 75),
  (6, "Frank", "Male", 5, 10, 55),
  (7, "Grace", "Female", 10, 19, 80),
  (8, "Henry", "Male", 4, 14, 50),
  (9, "Ivy", "Female", 7, 16, 60),
  (10, "Jack", "Male", 9, 18, 72),
  (11, "Kelly", "Female", 10, 20, 85),
  (12, "Leo", "Male", 8, 15, 65),
  (13, "Maria", "Female", 6, 12, 60),
  (14, "Nate", "Male", 9, 17, 68),
  (15, "Olivia", "Female", 7, 16, 75),
  (16, "Paul", "Male", 5, 10, 55),
  (17, "Quinn", "Female", 10, 19, 80),
  (18, "Ryan", "Male", 4, 14, 50),
  (19, "Sophia", "Female", 7, 16, 60),
  (20, "Tom", "Male", 9, 18, 72)
)

// Create DataFrame
val rdd = spark.sparkContext.parallelize(data)
val df = spark.createDataFrame(rdd, schema)

// Add AggMarks and Grade
val dfWithAggMarks = df
  .withColumn("AggMarks", col("Cat1") + col("Cat2") + col("Exam"))
  .withColumn("Grade", when(col("AggMarks") >= 80, "A")
    .when(col("AggMarks") >= 60, "B")
    .when(col("AggMarks") >= 40, "C")
    .otherwise("D"))

// Display the Performance List
dfWithAggMarks.show()

// 1. Performance list sorted ascending by the grade
val sortedByGrade = dfWithAggMarks.orderBy("Grade")
sortedByGrade.show()

// 2. List of the first top performers
val topPerformers = dfWithAggMarks.orderBy(col("AggMarks").desc).limit(5)
topPerformers.show()

// 3. The student who performed the best in Cat1
val bestInCat1 = dfWithAggMarks.orderBy(col("Cat1").desc).limit(1)
bestInCat1.show()

// 4. The student who performed the best in Cat2
val bestInCat2 = dfWithAggMarks.orderBy(col("Cat2").desc).limit(1)
bestInCat2.show()

// 5. The best-performing male
val bestMale = dfWithAggMarks.filter(col("Gender") === "Male").orderBy(col("AggMarks").desc).limit(1)
bestMale.show()

// 6. The best-performing female
val bestFemale = dfWithAggMarks.filter(col("Gender") === "Female").orderBy(col("AggMarks").desc).limit(1)
bestFemale.show()

// 7. Count the number of females in the class
val femaleCount = dfWithAggMarks.filter(col("Gender") === "Female").count()
println(s"Number of females in the class: $femaleCount")
