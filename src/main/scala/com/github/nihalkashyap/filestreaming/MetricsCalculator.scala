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

case class SensorReading(sensorId: String, temperature: Double)

object MetricsCalculator {

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("MetricsCalculator")
    val file = Paths.get("/home/nihalk/Downloads/sensor_temperature_10M.csv")
    val decoder: Flow[String, SensorReading, NotUsed] = Flow[String].map { s =>
      val attributes: Array[String] = s.split(",")
      val sensorId: String = attributes(0)
      val temperatureStr: String = attributes(1)
      SensorReading(sensorId, temperatureStr.toDouble)
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
