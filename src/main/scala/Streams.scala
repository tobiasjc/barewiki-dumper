package org.barewiki
package streams

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.slf4j.LoggerFactory

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.InputStream
import scala.collection.mutable.ListBuffer

object StreamLoader:
  val log = LoggerFactory.getLogger(this.getClass)

  def getStreamBuffer(inputStream: InputStream): ListBuffer[Byte] =
    var lb = ListBuffer[Byte]()

    synchronized {
      try lb ++= BZip2CompressorInputStream(inputStream).readAllBytes
      catch
        case e =>
          log.debug(e.getMessage)
          return null
    }
    lb

  def getStream(inputStream: InputStream): InputStream =
    BufferedInputStream(
      ByteArrayInputStream(getStreamBuffer(inputStream).toArray)
    )

  def getDocumentStream(inputStream: InputStream): InputStream =
    val content = getStreamBuffer(inputStream)

    if content == null then return null

    BufferedInputStream(
      ByteArrayInputStream(
        (content
          .prependAll("<document>".getBytes)
          .appendAll("</document>".getBytes))
          .toArray
      )
    )
