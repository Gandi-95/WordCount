package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object StreamingExamples extends Logging{

  def setStreamingLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized){
      logInfo("Setting log level to [WARN] for streaming example.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
