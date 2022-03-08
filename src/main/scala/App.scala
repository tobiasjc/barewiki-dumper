package org.barewiki
package dumper

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.LexerAction
import org.antlr.v4.runtime.atn.LexerActionType
import org.antlr.v4.runtime.tree._
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.barewiki.files.FileLoader
import org.barewiki.files.FileLoaderThreadFactory
import org.barewiki.processors.XMLStreamPageProcessor
import org.barewiki.sinks.SQL._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileReader
import java.io.InputStreamReader
import java.io.Reader
import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionService
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import javax.xml.parsers.SAXParserFactory
import javax.xml.stream.XMLEventReader
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLResolver
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.util.XMLEventConsumer
import scala.annotation.compileTimeOnly
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.runtime.stdLibPatches.language.future
import scala.util.Using

object App:
  val logger = LoggerFactory.getLogger(this.getClass)

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

    logger.info(
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

      logger.info(
        "File loaders turn received a total of {} pages",
        totalPagesCounter
      )

    logger.info(
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
