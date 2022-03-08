package org.barewiki
package files

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.barewiki.processors.XMLStreamPageProcessor
import org.barewiki.sinks._
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import org.barewiki.processors.XMLStreamHeaderProcessor
import java.io.ByteArrayInputStream
import java.io.BufferedInputStream
import org.barewiki.streams.StreamLoader
import javax.xml.stream.XMLInputFactory

object FileLoaderThreadFactory extends ThreadFactory:
  val name = "File Loader"
  val threadGroup = ThreadGroup("File Loaders")
  var idx = 0

  override def newThread(r: Runnable): Thread =
    idx += 1
    Thread(threadGroup, r, name + idx)

class FileLoader(
    path: Path,
    sinkSenders: Seq[SinkSender],
    streamLoadersCount: Int
) extends Callable[Long]:
  val log = LoggerFactory.getLogger(this.getClass)

  val executorService =
    Executors.newFixedThreadPool(streamLoadersCount, FileLoaderThreadFactory)

  val completionService = ExecutorCompletionService[Long](
    executorService
  )

  val xmlif = XMLInputFactory.newDefaultFactory
  val inputStream = Files.newInputStream(path)
  val header = StreamLoader.getStream(inputStream)

  val dbname = XMLStreamHeaderProcessor(sinkSenders(0), xmlif, header).run

  override def call: Long =

    val callables = ArrayBuffer[XMLStreamPageProcessor]()
    for t <- 0 until streamLoadersCount do
      callables += XMLStreamPageProcessor(
        dbname,
        sinkSenders(t),
        xmlif,
        inputStream
      )

    log.info("Running file loader over {}", path.getFileName)

    var filePages: Long = 0L

    for t <- 0 until streamLoadersCount do
      completionService.submit(callables(t))
    for t <- 0 until streamLoadersCount do
      filePages += completionService.take.get

    executorService.shutdown
    while !executorService.awaitTermination(100, TimeUnit.MILLISECONDS) do ()

    log.info("Total of {} pages on file {}", filePages, path.getFileName)

    filePages
