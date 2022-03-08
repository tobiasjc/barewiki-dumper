package org.barewiki
package processors

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.barewiki.sinks._
import org.barewiki.streams._
import org.slf4j.LoggerFactory

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionService
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import javax.annotation.processing.Completions
import javax.management.modelmbean.XMLParseException
import javax.xml.namespace.QName
import javax.xml.stream.XMLEventFactory
import javax.xml.stream.XMLEventReader
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamException
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.util.XMLEventAllocator
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

class XMLStreamHeaderProcessor(
    sinkSender: SinkSender,
    xmlif: XMLInputFactory,
    inputStream: InputStream
):
  val log = LoggerFactory.getLogger(this.getClass)
  val xmler = xmlif.createXMLEventReader(inputStream)

  def run: String =
    var dbname: String = null

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        xmlse.getName.getLocalPart match
          case "siteinfo" =>
            val siteinfo = handleSiteinfo
            dbname = siteinfo.get("dbname").orNull

            xmler.close
            return dbname
          case _ =>

    xmler.close
    dbname

  def handleSiteinfo: HashMap[String, String] =
    val siteinfo = HashMap[String, String]()
    var namespaces = ListBuffer[HashMap[String, String]]()

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        val tn = xmlse.getName.getLocalPart
        tn match
          case "namespaces" =>
            namespaces = handleNamespaces

          case "sitename" | "dbname" | "base" | "generator" | "case" =>
            if xmler.peek.isCharacters then
              siteinfo.put(tn, xmler.nextEvent.asCharacters.getData)

          case _ =>

      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "siteinfo" =>
            sinkSender.sendSiteinfo(siteinfo)

            namespaces
              .tapEach(ns => ns.put("dbname", siteinfo.get("dbname").orNull))
              .foreach(sinkSender.sendNamespace(_))

            return siteinfo
          case _ =>

    siteinfo

  def handleNamespaces: ListBuffer[HashMap[String, String]] =
    val namespaces = ListBuffer[HashMap[String, String]]()

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        val tn = xmlse.getName.getLocalPart
        tn match
          case "namespace" =>
            val namespace = HashMap[String, String]()

            val keyattr = xmlse.getAttributeByName(QName("key"))
            val caseattr = xmlse.getAttributeByName(QName("case"))

            namespace.put(
              "key",
              if keyattr == null then null else keyattr.getValue
            )

            namespace.put(
              "case",
              if caseattr == null then null else caseattr.getValue
            )

            if xmler.peek.isCharacters then
              namespace.put("name", xmler.nextEvent.asCharacters.getData)

            namespaces += namespace
          case _ =>

      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "namespaces" =>
            return namespaces
          case _ =>

    return namespaces

class XMLStreamPageProcessor(
    dbname: String,
    sinkSender: SinkSender,
    xmlif: XMLInputFactory,
    inputStream: InputStream
) extends Callable[Long]:

  val log = LoggerFactory.getLogger(this.getClass)
  var xmler: XMLEventReader = null

  override def call: Long =
    var pagesCounter = 0L

    while true do
      val stream = StreamLoader.getDocumentStream(inputStream)
      if stream != null then
        xmler = xmlif.createXMLEventReader(stream)
        pagesCounter += handleDocument
      else
        xmler.close
        return pagesCounter

    log.debug(
      "Stream {} processed a total of {} pages",
      Thread.currentThread.getName,
      pagesCounter
    )
    pagesCounter

  def handleDocument: Long =
    var pageCount = 0L

    while xmler.hasNext do
      var xmlne: XMLEvent = null

      try xmlne = xmler.nextEvent
      catch
        case e =>
          log.debug(e.getMessage)
          return pageCount

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        xmlse.getName.getLocalPart match
          case "page" =>
            val page = handlePage
            pageCount += 1
          case _ =>

      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "document" =>
            xmler.close
            sinkSender.dispatch
            return pageCount
          case _ =>
    pageCount

  def handlePage: HashMap[String, String] =
    val page = HashMap[String, String]()

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        val tn = xmlse.getName.getLocalPart
        tn match
          case "revision" =>
            val revision = handleRevision
            val optRevisionId = revision.get("id")

            if optRevisionId.isDefined then
              page.put("revision_id", optRevisionId.get)
          case "redirect" =>
            val attr = xmlse.getAttributeByName(QName("title"))

            if attr != null then page.put("redirect", attr.getValue)
          case "id" | "ns" | "title" =>
            if xmler.peek.isCharacters then
              page.put(tn, xmler.nextEvent.asCharacters.getData)
          case _ =>
      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "page" =>
            page.put("dbname", dbname)
            sinkSender.sendPage(page)
            return page
          case _ =>

    page.put("dbname", dbname)
    page

  def handleRevision: HashMap[String, String] =
    val revision = HashMap[String, String]()

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        val tn = xmlse.getName.getLocalPart
        tn match
          case "contributor" =>
            val contributor = handleContributor
            val optContributorId = contributor.get("id")

            if optContributorId.isDefined then
              revision.put(
                "contributor_id",
                optContributorId.get
              )
          case "text" =>
            val attr = xmlse.getAttributeByName(QName("bytes"))

            if attr != null then revision.put("bytes", attr.getValue)

            if xmler.peek.isCharacters then
              revision.put("text", xmler.nextEvent.asCharacters.getData)
          case "id" | "parentid" | "timestamp" | "comment" | "model" | "sha1" |
              "format" =>
            if xmler.peek.isCharacters then
              revision.put(tn, xmler.nextEvent.asCharacters.getData)
          case _ =>

      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "revision" =>
            revision.put("dbname", dbname)
            sinkSender.sendRevision(revision)
            return revision
          case _ =>

    revision.put("dbname", dbname)
    revision

  def handleContributor: HashMap[String, String] =
    val contributor = HashMap[String, String]()

    while xmler.hasNext do
      val xmlne = xmler.nextEvent

      if xmlne.isStartElement then
        val xmlse = xmlne.asStartElement

        val tn = xmlse.getName.getLocalPart
        tn match
          case "id" | "username" =>
            val ne = xmler.nextEvent
            contributor.put(tn, ne.asCharacters.getData)
          case _ =>

      else if xmlne.isEndElement then
        val xmlee = xmlne.asEndElement

        xmlee.getName.getLocalPart match
          case "contributor" =>
            contributor.put("dbname", dbname)
            sinkSender.sendContributor(contributor)
            return contributor
          case _ =>

    contributor.put("dbname", dbname)
    contributor
