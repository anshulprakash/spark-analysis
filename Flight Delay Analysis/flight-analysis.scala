

val airlines = sc.textFile("airlines.csv")
val flights = sc.textFile("flights.csv")
val airports = sc.textFile("airports.csv")

val airlinesWoHeader = airlines.filter(x => !x.contains("Description"))

def parseLookup(row: String): (String, String)={
	val x = row.replace("/","").split(",")
	(x(0), x(1))
}

val airlinesParsed = airlinesWoHeader.map(parseLookup)

import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate

case class Flight(date: String,
                  airline: String ,
                  flightnum: String,
                  origin: String ,
                  dest: String ,
                  dep: String,
                  dep_delay: Double,
                  arv: String,
                  arv_delay: Double ,
                  airtime: Double ,
                  distance: Double
                   )

def parse(row: String): Flight={

  val fields = row.split(",")
  //val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
  //val timePattern = DateTimeFormat.forPattern("HHmm")

  //val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
  val date: String = fields(0)
  val airline: String = fields(1)
  val flightnum: String = fields(2)
  val origin: String = fields(3)
  val dest: String = fields(4)
  //val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
  val dep: String = fields(5)
  val dep_delay: Double = fields(6).toDouble
  //val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
  val arv: String = fields(7)
  val arv_delay: Double = fields(8).toDouble
  val airtime: Double = fields(9).toDouble
  val distance: Double = fields(10).toDouble
  
  Flight(date,airline,flightnum,origin,dest,dep,
         dep_delay,arv,arv_delay,airtime,distance)

    }

val flightsParsed = flights.map(parse)

val totalDistance = flightsParsed.map(_.distance).reduce((x,y) => x + y)

val avgDistance = totalDistance / flightsParsed.count()

println(avgDistance)

val sumCount=flightsParsed.map(_.dep_delay).aggregate((0.0,0))((acc, value) => (acc._1 + value, acc._2+1),
                                                           (acc1,acc2) => (acc1._1+acc2._1,acc1._2+acc2._2))

val avgDepDelay = sumCount._1 / sumCount._2

flightsParsed.map(x => (x.dep_delay/60).toInt).countByValue()

val airportDelays = flightsParsed.map(x => (x.origin,x.dep_delay))
val airportTotalDelay=airportDelays.reduceByKey((x,y) => x+y)
val airportCount=airportDelays.mapValues(x => 1).reduceByKey((x,y) => x+y)
val airportSumCount=airportTotalDelay.join(airportCount)
val airportAvgDelay=airportSumCount.mapValues(x => x._1/x._2.toDouble)
airportAvgDelay.sortBy(-_._2).take(10)

