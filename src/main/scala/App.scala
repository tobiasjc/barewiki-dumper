package org.barewiki
package dumper

import org.barewiki.files.FileLoader
import org.barewiki.files.FileLoaderThreadFactory
import org.barewiki.sinks.SQL.SinkAssert
import org.barewiki.sinks.SQL.SinkSender
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.Properties
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.jdk.CollectionConverters._

object App:
  val log = LoggerFactory.getLogger(this.getClass)

  val props: Properties = new Properties()

  def main(args: Array[String]): Unit =
    val start = Instant.now

    loadProperties(args)
    val articlesPaths = getArticles

    val filesThreadsWish = props.getProperty("files.threads", "3").toInt

    val filesThreads =
      if filesThreadsWish > articlesPaths.length then articlesPaths.length
      else filesThreadsWish
    val streamLoaders = props.getProperty("loader.threads", "2").toInt
    val sinkSenders = allocateSinkSenders(filesThreads, streamLoaders)

    val filesExecutorService = Executors.newFixedThreadPool(
      filesThreads,
      FileLoaderThreadFactory
    )

    val filesCompletionService = ExecutorCompletionService[Long](
      filesExecutorService
    )

    log.info(
      "Executing over files on repository {}",
      props.getProperty("data.repository.articles", "/var/wikidata/bz2/")
    )

    val sinkAssert = SinkAssert(props)
    var doneFilesCounter = 0
    var totalPagesCounter = 0L
    while articlesPaths.hasNext do
      sinkAssert.assert

      var sentFilesCounter = 0
      for i <- 0 until filesThreads do
        if articlesPaths.hasNext then
          sentFilesCounter += 1
          filesCompletionService.submit(
            FileLoader(
              articlesPaths.next,
              sinkSenders(i),
              streamLoaders
            )
          )

      for t <- 0 until sentFilesCounter do
        val receivedPages = filesCompletionService.take.get
        totalPagesCounter += receivedPages
        doneFilesCounter += 1

      log.info(
        "File loaders turn received a total of {} pages",
        totalPagesCounter
      )

    log.info(
      "Total of {} pages processed from {} files",
      totalPagesCounter,
      doneFilesCounter
    )

    sinkAssert.close
    filesExecutorService.shutdown
    while !filesExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS) do
      (
    )

  def loadProperties(args: Array[String]): Unit =
    props.load(Source.fromResource("barewiki-default.properties").reader)
    if args.length == 1 then
      val path = args(0)
      props.load(Source.fromFile(path).reader)

  def allocateSinkSenders(files: Int, loaders: Int): Seq[Seq[SinkSender]] =
    Seq
      .fill[Seq[SinkSender]](files)(
        Seq.fill[SinkSender](loaders)(SinkSender(props))
      )

  def getArticles: Iterator[Path] =
    Files
      .newDirectoryStream(
        Paths.get(
          props.getProperty("data.repository.articles", "/var/wikidata/bz2/")
        )
      )
      .asScala
      .map(_.toAbsolutePath)
      .iterator
