import org.apache.spark.sql.SparkSession
import java.time.LocalDate

object Main {

  // Implicit ordering per LocalDate
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
  
  case class Location(lat: Double, lon: Double) {
    override def toString: String = s"($lat, $lon)"
  }
  
  case class LocationPair(loc1: Location, loc2: Location) {
    def normalized: LocationPair = {
      if (loc1.lat < loc2.lat || (loc1.lat == loc2.lat && loc1.lon <= loc2.lon))
        this
      else
        LocationPair(loc2, loc1)
    }
    override def toString: String = s"($loc1, $loc2)"
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Earthquake Analysis")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions =
      if (args.length > 2) args(2).toInt else sc.defaultParallelism * 3

    val startTime = System.currentTimeMillis()

    // Caricamento e parsing
    val events = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .rdd
      .repartition(numPartitions)
      .map { row =>
        val lat = Math.round(row.getAs[String]("latitude").toDouble * 10.0) / 10.0
        val lon = Math.round(row.getAs[String]("longitude").toDouble * 10.0) / 10.0
        val date = LocalDate.parse(row.getAs[String]("date").substring(0, 10))
        ((lat, lon, date), 1)
      }
      .reduceByKey(_ + _)
      .map { case ((lat, lon, date), _) => (date, Location(lat, lon)) }
      .cache()
    
    // Raggruppa per data con aggregateByKey
    val eventsByDate = events
      .aggregateByKey(Set.empty[Location])(
        (set, loc) => set + loc,
        (set1, set2) => set1 ++ set2
      )
      .filter(_._2.size > 1)
      .cache()
    
    // Genera coppie e conta co-occorrenze
    val pairCounts = eventsByDate
      .flatMap { case (_, locations) =>
        val locList = locations.toList
        for {
          i <- locList.indices
          j <- i + 1 until locList.size
        } yield (LocationPair(locList(i), locList(j)).normalized, 1)
      }
      .reduceByKey(_ + _)
    
    // Trova coppia vincente
    val (winningPair, maxCount) = pairCounts.reduce { (a, b) => 
      if (a._2 > b._2) a else b 
    }
    
    // Trova date co-occorrenze con sort distribuito
    val winningDates = eventsByDate
      .filter { case (_, locations) =>
        locations.contains(winningPair.loc1) && locations.contains(winningPair.loc2)
      }
      .keys
      .sortBy(identity[LocalDate])
      .collect()
    
    val executionTime = System.currentTimeMillis() - startTime
    
    // Output risultato
    val result = Seq(winningPair.toString) ++ winningDates.map(_.toString)
    sc.parallelize(result, 1).saveAsTextFile(s"$outputPath/result")
    
    sc.parallelize(Seq(executionTime.toString), 1).saveAsTextFile(s"$outputPath/time")
    
    events.unpersist()
    eventsByDate.unpersist()
    
    spark.stop()
  }
}