package com.github.nihalkashyap.filestreaming

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.IOResult

import java.nio.file.Paths
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class SensorReading(sensorId: Option[String], temperature: Option[Double])

object MetricsCalculator {

  private def toDouble(str: String): Option[Double] = Try {
    str.toDouble
  } match {
    case Failure(_) => None
    case Success(value) => Some(value)
  }

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("MetricsCalculator")
    val file = Paths.get("/home/nihalk/Downloads/sensor_temperature_10M.csv")
    val decoder: Flow[String, SensorReading, NotUsed] = Flow[String].map { s =>
      val elems: Array[String] = s.split(",")
      val sensorId: Option[String] = elems.headOption
      val temperatureStr: Option[String] = elems.lift(1)
      SensorReading(sensorId, temperatureStr.flatMap(toDouble))
    }

    val graph = FileIO
      .fromPath(file)
      .via(Framing.delimiter(ByteString(System.lineSeparator()), 256, allowTruncation = true))
      .map(byteStringLine => byteStringLine.utf8String)
      .filter(line => line.nonEmpty && !line.startsWith("sensor_id"))
      .via(decoder)
      .runForeach(r => println(r))

    implicit val ec: ExecutionContextExecutor = system.dispatcher
    graph.onComplete(_ => system.terminate())

    try {
      Await.result(graph, Duration.Inf)
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }
}
