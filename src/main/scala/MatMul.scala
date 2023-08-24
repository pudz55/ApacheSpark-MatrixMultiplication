import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


case class MatrixA(t: Long, ndx: Long, value: Double) extends Serializable {}

case class MatrixB(t: Long, ndx: Long, value: Double) extends Serializable {}

object Multiply extends Serializable {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", "1000000")
    conf.set("spark.logConf", "false")
    conf.set("spark.eventLog.enabled", "false")

    // Read input matrices from text files
    val matrixA = sc.textFile(args(0)).map(line => {
      val a = line.split(",")
      MatrixA(a(0).toLong, a(1).toLong, a(2).toDouble)
    })

    val matrixB = sc.textFile(args(1)).map(line => {
      val a = line.split(",")
      MatrixB(a(0).toLong, a(1).toLong, a(2).toDouble)
    })

    val result = matrixA.map(a => (a.ndx, (a.t, a.value))).join(matrixB.map(b => (b.t, (b.ndx, b.value))))
      .map { case (_, ((tA, valueA), (ndxB, valueB))) => ((tA, ndxB), valueA * valueB) }
      .reduceByKey(_ + _)
      .sortByKey()
      .map { case ((t, ndx), value) => s"$t,$ndx,$value" }

    // Save the result to output file
    result.saveAsTextFile(args(2))

    sc.stop()
  }
}
