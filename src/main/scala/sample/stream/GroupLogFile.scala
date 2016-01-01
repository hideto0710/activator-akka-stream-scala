package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, ActorMaterializer}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
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
      source ~> broadcast.in
      for ((l, i) <- LogLevels.view.zipWithIndex)
        broadcast.out(i) ~> Flow[String].
          filter {
            case LoglevelPattern(level) => level == l
            case _ => l == "OTHER"
          }.
          map(line => ByteString(line + "\n")).
          toMat(FileIO.toFile(new File(s"target/log-$l.txt")))((_, bytesWritten) => bytesWritten)
      ClosedShape
    }
    RunnableGraph.fromGraph(graph).run()
  }
}
