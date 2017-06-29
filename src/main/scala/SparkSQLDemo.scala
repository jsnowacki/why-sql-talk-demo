import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import scala.util.control.Breaks._

/**
  * Created by jnowac01 on 27.06.2017.
  */
object SparkSQLDemo {
    case class SalesRow (retailerCountry: String,
                         orderMethodType: String,
                         retailerType: String,
                         productLine: String,
                         productType: String,
                         product: String,
                         year: Int,
                         quarter: String,
                         revenue: Double,
                         quantity: Int,
                         grossMargin: Double)



    val csvFile = "data/WA_Sales_Products_2012-14.csv"

    def fromCsv(line: String): SalesRow = {
        val cols = line.split(",", -1).map(_.trim)

        SalesRow(
            cols(0),
            cols(1),
            cols(2),
            cols(3),
            cols(4),
            cols(5),
            Try(cols(6).toInt).getOrElse(0),
            cols(7),
            Try(cols(8).toDouble).getOrElse(Double.NaN),
            Try(cols(9).toInt).getOrElse(0),
            Try(cols(10).toDouble).getOrElse(Double.NaN)
        )
    }

    def readCsv(fileName: String): List[SalesRow] = {
        val iter = Source.fromFile(fileName).getLines

        // Drop the first one
        iter.next

        iter.map(fromCsv).toList
    }

    case class Result1 (productLine: String, product: String, revenue: Double)

    def select1(salesRows: List[SalesRow]): List[Result1] = {
        var i = 0
        val results = new ListBuffer[Result1]()
        breakable {
            for (r <- salesRows) {
                if (r.year == 2012) {
                    i += 1
                    results.append(
                        Result1(r.productLine,
                                r.product,
                                r.revenue))
                }

                if (i >= 10)
                    break
            }
        }
        results.toList
    }

    case class Result2 (productLine: String, product: String, revenue: Double, avgRevenue: Double)

    def select2(salesRows: List[SalesRow]): List[Result2] = {
        val sorted = salesRows.sortBy(-_.revenue)
        val results1 = select1(sorted)
        var revenueSum = 0.0
        for (r <- salesRows) {
            revenueSum += r.revenue
        }
        val avgRevenue = revenueSum/salesRows.size
        val results2 = new ListBuffer[Result2]()
        for (r <- results1) {
            results2.append(
                Result2(r.productLine,
                        r.product,
                        r.revenue,
                        avgRevenue
                    )
            )
        }
        results2.toList
    }

    def select2functional(salesRows: List[SalesRow]): List[Result2] = {
        val sumRevenue = salesRows
            .map(_.revenue)
            .sum
        val avgRevenue = sumRevenue / salesRows.size
        salesRows.filter(_.year == 2012)
            .sortBy(- _.revenue)
            .map(r =>
                Result2(r.productLine,
                        r.product,
                        r.revenue,
                        avgRevenue)
            ).take(10)
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("SparkSQLDemo")
            .master("local[*]")
            .getOrCreate()
        import spark.implicits._

        val sales = spark.read
            .options(Map("header" -> "true", "inferSchema" -> "true"))
            .csv(csvFile)
        sales.show()
        sales.printSchema()
        sales.createTempView("sales")

        val salesRows = readCsv(csvFile)
        salesRows.take(10).foreach(println)

        var query = ""

        println("\n**** Select 1 (SQL) ****")
        query = """
                      |SELECT
                      |    `Product line`,
                      |    Product,
                      |    Revenue
                      |FROM sales
                      |WHERE Year = 2012
                      |LIMIT 10
                    """.stripMargin
        spark.sql(query).show()

        println("\n**** Select 1 (Scala) ****")
        select1(salesRows).foreach(println)

        println("\n**** Select 2 (SQL) ****")
        query = """
                      |SELECT
                      |    `Product line`,
                      |    Product,
                      |    Revenue,
                      |    (SELECT AVG(Revenue) FROM sales)
                      |AS `Avg Revenue`
                      |FROM sales
                      |WHERE Year = 2012
                      |ORDER BY Revenue DESC
                      |LIMIT 10
                    """.stripMargin
        spark.sql(query).show()

        println("\n**** Select 2 (Scala) ****")
        select2(salesRows).foreach(println)

        println("\n**** Select 2 (Scala - functional) ****")
        select2functional(salesRows).foreach(println)

        println("\n**** Group By (SQL) ****")
        query = """
                  |SELECT
                  |    `Product line`,
                  |    AVG(Revenue) AS `Avg Revenue`,
                  |    SUM(Quantity) AS `Sum Quantity`,
                  |    MAX(Quantity) AS `Max Quantity`
                  |FROM sales
                  |GROUP BY `Product line`
                  |ORDER BY `Avg Revenue` DESC
                """.stripMargin
        spark.sql(query).show()


        println("\n**** Windows functions (Spark SQL) ****")
        val w = Window.partitionBy("Product line")
            .orderBy($"Revenue".desc)

        val df = sales.select(
                $"Product line",
                $"Revenue",
                rank.over(w).as("Rank"),
                ($"Revenue" - expr("(SELECT AVG(Revenue) FROM sales)")).as("Diff Revenue")
            ).where("Rank <= 3")
        df.show()
    }
}
