import org.apache.spark.sql.SparkSession
import java.time.LocalDate

object Main {

  // Ordinamento implicito per LocalDate basato sui giorni dall'epoca
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
  
  case class Location(lat: Double, lon: Double) {
    override def toString: String = s"($lat, $lon)"
  }
  
  case class LocationPair(loc1: Location, loc2: Location) {
    // Normalizza la coppia per garantire un ordine consistente
    // (importante per il conteggio delle co-occorrenze)
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
    // Se non specificato, usa 3x il parallelismo di default
    val numPartitions =
      if (args.length > 2) args(2).toInt else sc.defaultParallelism * 3

    val startTime = System.currentTimeMillis()

    // Carica CSV e arrotonda coordinate a 0.1 gradi di precisione
    // Raggruppa per (lat, lon, data) eliminando duplicati nello stesso giorno
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
    
    // Raggruppa per data creando un Set di Location per ogni giorno
    // Filtra solo i giorni con almeno 2 eventi (altrimenti non ci sono coppie)
    val eventsByDate = events
      .aggregateByKey(Set.empty[Location])(
        (set, loc) => set + loc,
        (set1, set2) => set1 ++ set2
      )
      .filter(_._2.size > 1)
      .cache()
    
    // Genera tutte le coppie di location per ogni data e conta le co-occorrenze
    // normalized() garantisce che (A,B) e (B,A) siano trattate come la stessa coppia
    val pairCounts = eventsByDate
      .flatMap { case (_, locations) =>
        val locList = locations.toList
        for {
          i <- locList.indices
          j <- i + 1 until locList.size
        } yield (LocationPair(locList(i), locList(j)).normalized, 1)
      }
      .reduceByKey(_ + _)
    
    // Trova la coppia con il maggior numero di co-occorrenze
    val (winningPair, maxCount) = pairCounts.reduce { (a, b) => 
      if (a._2 > b._2) a else b 
    }
    
    // Estrae tutte le date in cui la coppia vincente è co-occorsa
    // e le ordina cronologicamente
    val winningDates = eventsByDate
      .filter { case (_, locations) =>
        locations.contains(winningPair.loc1) && locations.contains(winningPair.loc2)
      }
      .keys
      .sortBy(identity[LocalDate])
      .collect()
    
    val executionTime = System.currentTimeMillis() - startTime
    
    // Salva i risultati: prima la coppia vincente, poi le date
    val result = Seq(winningPair.toString) ++ winningDates.map(_.toString)
    sc.parallelize(result, 1).saveAsTextFile(s"$outputPath/result")
    
    // Salva il tempo di esecuzione separatamente
    sc.parallelize(Seq(executionTime.toString), 1).saveAsTextFile(s"$outputPath/time")
    
    // Libera la memoria cache
    events.unpersist()
    eventsByDate.unpersist()
    
    spark.stop()
  }
}