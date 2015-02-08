import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.joda.time.{DateTimeConstants, DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import breeze.linalg._
import breeze.linalg.DenseVector._
import breeze.numerics._


case class Report(iteration: Int, alpha: Double, model: DenseVector[Double],
                  predition: Double)

object SimpleApp extends App {
  val conf = new SparkConf().setAppName("SimpleApp")
  val sc = new SparkContext(conf)
  val daily_file = "in_out_count_daily.csv"

  val raw = sc.textFile(daily_file)
  val raw_taipei = raw.filter(_.contains("台北"))

  //val taipei = taipei_in_out(raw_taipei)
  //val taipei = taipei_in_out(raw_taipei).cache()
  val taipei: RDD[(DateTime, Int, Int)] = taipei_in_out(raw_taipei).
    persist(StorageLevel.MEMORY_ONLY)

  // RDD[(secs, in-counts)]
  val mons: RDD[(Long, Int)] = taipei.filter(
    DateTimeConstants.MONDAY == _._1.getDayOfWeek ).map({
    x: (DateTime, Int, Int) =>
    (x._1.getMillis, x._2)
  })

  val (normalized_mons, (mean, scale)) = normalize(mons)
  val normalized = normalized_mons.cache()


  /*
  val model = BGD(normalized, 1000, 0.1)
  evaluate(normalized, model, 10)

  println(s"model = ${model(0)}, ${model(1)}====")

  val today = new DateTime().getMillis.toDouble
  val pred = DenseVector((today - mean) / scale, 1.0) dot model
  println(s"predition for ${today} is $pred")
  */

  val r0 = todayReport(100, 0.1, normalized)
  val r1 = todayReport(500, 0.1, normalized)
  val r2 = todayReport(1000, 0.1, normalized)
  val r3 = todayReport(1500, 0.1, normalized)
  val r4 = todayReport(2000, 0.1, normalized)
  val r5 = todayReport(2500, 0.1, normalized)

  println(r0)
  println(r1)
  println(r2)
  println(r3)
  println(r4)
  println(r5)

  /*
  mons.collect().take(10).foreach ({ e =>
    println(e)
  })

  val suns = taipei.filter({ x: (DateTime, Int, Int) =>
    DateTimeConstants.SUNDAY == x._1.getDayOfWeek
  })
  */

  // =================================================

  def todayReport(iteration: Int, alpha: Double, data: RDD[(Double, Double)])
  : Report = {
    val model = BGD(data, iteration, alpha)
    val today = new DateTime().getMillis.toDouble
    val pred = DenseVector((today - mean) / scale, 1.0) dot model
    Report(iteration, alpha, model, pred)
  }

  def taipei_in_out(data: RDD[String]): RDD[(DateTime, Int, Int)] = {
    data.map({ s =>
      val words = s.split(",")
      val fmt = DateTimeFormat.forPattern("yyyyMMdd").withZone(
        DateTimeZone.getDefault)
      (fmt.parseDateTime(words(0)), words(3).toInt, words(4).toInt)
    }).filter( x => x._2 >= 0 && x._3 >= 0)
  }

  def normalize(in: RDD[(Long, Int)])
  : (RDD[(Double, Double)], (Double, Double)) = {
    val in2 = in.map(x => (x._1.toDouble, x._2.toDouble)).cache()
    val fst = in2.map(_._1).cache()

    val fst_mean = fst.mean() // import SparkContext._
    val fst_scale = fst.max() - fst.min()

    (in2.map({ case (x, y) =>
      ((x - fst_mean) / fst_scale, y)
    }), (fst_mean, fst_scale))
  }

  // Batch Gradient Descent
  def BGD(in: RDD[(Double, Double)], iteration: Int, alpha: Double):
  DenseVector[Double] = {

    var theta = DenseVector.zeros[Double](2)

    val count = in.count().toDouble // DenseVector[Double].:/(b: Double)

    for (i <- 1 to iteration) {
      val tmp: RDD[DenseVector[Double]] = in.map({ p =>
        val x = DenseVector(p._1, 1.0)
        // Either import implicits in breeze.linalg.DenseVector._ for dot
        // Or (x.t * theta)
        x * (x.dot(theta) - p._2)
      })

      val summed: DenseVector[Double] = tmp.reduce((a, b) => a + b)
      val diff = summed * alpha / count
      println(s"theta($i): ${theta(0)}, ${theta(1)}============================")
      theta = theta - diff
    }
    theta
  }

  def evaluate(in: RDD[(Double, Double)],
               model: DenseVector[Double], num: Int) = {
    in.take(num).foreach({ case (x, y) =>
      val pred = DenseVector(x, 1.0) dot model
      println(s"$x, real=$y, predited=$pred")
    })
  }

}

