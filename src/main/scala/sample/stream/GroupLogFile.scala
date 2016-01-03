package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{SinkShape, ActorMaterializer}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.util.{Success, Failure}

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r
    val LogLevels = Seq("DEBUG", "INFO", "WARN", "ERROR", "OTHER")

    // read lines from a log file
    val logFile = new File("src/main/resources/logfile.txt")

    val source = FileIO.fromFile(logFile).
      // parse chunks of bytes into lines
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String)

    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[String](LogLevels.size))
      for ((l, i) <- LogLevels.view.zipWithIndex)
        broadcast.out(i) ~> Flow[String].
          filter {
            case LoglevelPattern(level) => level == l
            case _ => l == "OTHER"
          }.
          map(line => ByteString(line + "\n")).
          toMat(FileIO.toFile(new File(s"target/log-$l.txt")))((_, bytesWritten) => bytesWritten)
      SinkShape(broadcast.in)
    }
    val materialized = Sink.fromGraph(graph).runWith(source)

    materialized.onComplete {
      case Success(_) =>
        system.shutdown()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}")
        system.shutdown()
    }
  }
}
