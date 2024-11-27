import org.apache.spark.SparkContext

// // . Create a file in Marks/marks.txt in your home directory with the following
// 45,78,65,45,43,23,32,54,65,23,23,34,45,76,45,56,65,45,67,76,56
// 45,78,65,45,43,23,32,54,65,23,23,34,45,76,45,56,65,45,67,76,56

// 2. Create a marksRDD based on the marks.txt file
val marksRDD=sc.textFile("~/Desktop/systems/scala/apache/marks.txt")

val marks=marksRDD.flatMap(line=>line.split(","))

marks.foreach(println)

