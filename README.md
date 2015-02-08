
a simple batch gradient descent(BGD) demo using Spark

$ sbt assembly


$ 00_project/spark-1.2.0-bin-hadoop2.4/bin/spark-submit 
  --class "SimpleApp" --master local[2] 
  target/scala-2.10/cf150204-assembly-1.0.jar

