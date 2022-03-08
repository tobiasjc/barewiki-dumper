package org.barewiki
package streams

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.slf4j.LoggerFactory

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ThreadFactory
import scala.collection.mutable.ArrayBuffer

object StreamLoader:
  val log = LoggerFactory.getLogger(this.getClass)

  def getStreamBuffer(inputStream: InputStream): ArrayBuffer[Byte] =
    var ba = ArrayBuffer[Byte]()

    synchronized {
      try ba ++= BZip2CompressorInputStream(inputStream).readAllBytes
      catch
        case e =>
          log.debug(e.getMessage)
          return null
    }
    ba

  def getStream(inputStream: InputStream): InputStream =
    BufferedInputStream(
      ByteArrayInputStream(getStreamBuffer(inputStream).toArray)
    )

  def getDocumentStream(inputStream: InputStream): InputStream =
    val content = getStreamBuffer(inputStream)

    if content == null then return null

    val ba = ArrayBuffer[Byte]()
    ba ++= "<document>".getBytes
    ba ++= content
    ba ++= "</document>".getBytes

    BufferedInputStream(ByteArrayInputStream(ba.toArray))
