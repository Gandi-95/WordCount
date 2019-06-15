package SparkStreaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source


object DataSourceSocket {

  def index(length:Int)={
    val rdm = new java.util.Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    val fileName ="D:\\Intellij Workspace\\WordCount\\src\\main\\WordCount\\SparkStreaming\\window_nc\\readme.txt"
    val port =9999
    val lines = Source.fromFile(fileName).getLines().toList
    val rowCount = lines.length

    val lister = new ServerSocket(port)
    while (true){
      val socket = lister.accept()
      new Thread(){
        override def run(): Unit = {
          val out = new PrintWriter(socket.getOutputStream,true)
          while (true){
            Thread.sleep(1000)
            val content = lines(index(rowCount))
            out.write(content+"\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }

  }
}
