package org.barewiki
package sinks

import org.slf4j.LoggerFactory

import java.io.Closeable
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.Using

trait SinkSender extends Closeable:
  def sendSiteinfo(siteinfo: HashMap[String, String]): Unit
  def sendNamespace(namespace: HashMap[String, String]): Unit
  def sendPage(page: HashMap[String, String]): Unit
  def sendRevision(revision: HashMap[String, String]): Unit
  def sendContributor(contributor: HashMap[String, String]): Unit
  def dispatch: Unit
  def close: Unit

trait SinkAssert extends Closeable:
  def assertColumnTables(
      database: String,
      table: String,
      columns: ArrayBuffer[(String, String)],
      composite: ArrayBuffer[String]
  ): Unit

  def assertIndexes(
      database: String,
      table: String,
      indexes: ArrayBuffer[(String, ArrayBuffer[String])]
  ): Unit

  def assertForeignKeys(
      database: String,
      table: String,
      foreignKeys: ArrayBuffer[
        (String, ArrayBuffer[String], String, ArrayBuffer[String])
      ]
  ): Unit

  def assert: Unit

  def close: Unit

object SQL:
  // SITEINFO CONFIG
  val siteinfoColumns = ArrayBuffer(
    ("dbname", "text primary key"),
    ("sitename", "text"),
    ("base", "text"),
    ("generator", "text"),
    ("case", "text")
  )
  val siteinfoIndexes = ArrayBuffer(
    ("sitename_dbname_base_idx", ArrayBuffer("sitename", "dbname", "base"))
  )

  // NAMESPACES CONFIG
  val namespacesColumns = ArrayBuffer(
    ("key", "smallint not null"),
    ("dbname", "text not null"),
    ("case", "text"),
    ("name", "text")
  )
  val namespacesIndexes = ArrayBuffer(
    ("name_idx", ArrayBuffer("name"))
  )
  val namespacesForeignKeys = ArrayBuffer(
    ("dbname_fk", ArrayBuffer("dbname"), "siteinfo", ArrayBuffer("dbname"))
  )
  val namespacesComposite = ArrayBuffer("key", "dbname")

  // PAGE CONFIG
  val pageColumns = ArrayBuffer(
    ("id", "bigint not null"),
    ("dbname", "text not null"),
    ("ns", "smallint"),
    ("title", "text"),
    ("redirect", "text"),
    ("revision_id", "bigint")
  )
  val pageIndexes = ArrayBuffer(
    ("title_redirect_idx", ArrayBuffer("title", "redirect"))
  )
  val pageForeignKeys = ArrayBuffer(
    (
      "ns_fk",
      ArrayBuffer("ns, dbname"),
      "namespaces",
      ArrayBuffer("key, dbname")
    ),
    (
      "revision_id_fk",
      ArrayBuffer("revision_id", "dbname"),
      "revision",
      ArrayBuffer("id", "dbname")
    ),
    ("dbname_fk", ArrayBuffer("dbname"), "siteinfo", ArrayBuffer("dbname"))
  )
  val pageComposite = ArrayBuffer("id", "dbname")

  // REVISION CONFIG
  val revisionColumns = ArrayBuffer(
    ("id", "bigint not null"),
    ("dbname", "text not null"),
    ("parentid", "bigint"),
    ("timestamp", "timestamp"),
    ("comment", "text"),
    ("model", "text"),
    ("format", "text"),
    ("text", "text"),
    ("bytes", "bigint"),
    ("sha1", "text"),
    ("contributor_id", "bigint")
  )
  val revisionIndexes = ArrayBuffer(
    ("id_parentid_idx", ArrayBuffer("id", "parentid"))
  )
  val revisionForeignKeys = ArrayBuffer(
    (
      "contributor_id_fk",
      ArrayBuffer("contributor_id", "dbname"),
      "contributor",
      ArrayBuffer("id", "dbname")
    ),
    ("dbname_fk", ArrayBuffer("dbname"), "siteinfo", ArrayBuffer("dbname"))
  )
  val revisionComposite = ArrayBuffer("id", "dbname")

  // CONTRIBUTOR CONFIG
  val contributorColumns = ArrayBuffer(
    ("id", "bigint not null"),
    ("dbname", "text not null"),
    ("username", "text")
  )
  val contributorIndexes = ArrayBuffer(
    ("username_idx", ArrayBuffer("username"))
  )
  val contributorForeignKeys = ArrayBuffer(
    ("dbname_fk", ArrayBuffer("dbname"), "siteinfo", ArrayBuffer("dbname"))
  )
  val contributorComposite = ArrayBuffer("id", "dbname")

  val dtf = DateTimeFormatter.ISO_INSTANT

  object Connector:
    val logger = LoggerFactory.getLogger(this.getClass)

    def getConnection(props: Properties): Connection =
      val url =
        f"jdbc:${props.getProperty("database.vendor", "postgresql")}://${props
          .getProperty("database.host", "localhost")}:${props.getProperty("database.port", "5432")}/${props
          .getProperty("database.name", "barewiki")}?user=${props
          .getProperty("database.user.name", "barewiki")}&password=${props
          .getProperty("database.user.password", "barewiki")}"

      DriverManager.getConnection(url)

  class SinkAssert(props: Properties) extends org.barewiki.sinks.SinkAssert:
    val logger = LoggerFactory.getLogger(this.getClass)
    val conn = Connector.getConnection(props)

    def assertColumnTables(
        database: String,
        table: String,
        columns: ArrayBuffer[(String, String)],
        composite: ArrayBuffer[String]
    ): Unit =
      logger.debug("Asserting columns for table {}", table)

      val md = conn.getMetaData
      Using(md.getTables(database, null, table, Array("TABLE"))) { rs =>
        {
          if (!rs.isBeforeFirst) then
            conn.createStatement.execute(
              f"create table ${table} ${columns
                .map(c => c._1.mkString("\"", "", "\"") + " " + c._2)
                .mkString("(", ", ", ")")}"
            )

            if composite != null then
              conn.createStatement.execute(
                f"alter table ${table} add primary key ${composite.mkString("(", ", ", ")")}"
              )
          else
            columns
              .filter(c => {
                try
                  rs.findColumn(c._1)
                  false
                catch case _ => true
              })
              .foreach(c =>
                conn.createStatement
                  .execute(f"alter table ${table} add column ${c._1} ${c._2}")
              )
        }
      }

    def assertIndexes(
        database: String,
        table: String,
        indexes: ArrayBuffer[(String, ArrayBuffer[String])]
    ): Unit =
      logger.debug("Asserting indexes for table {}", table)

      val md = conn.getMetaData
      Using(md.getIndexInfo(database, null, table, false, false)) { rs =>
        {
          while (rs.next) do
            indexes.filterInPlace(p => p._1 != rs.getString("INDEX_NAME"))

          indexes.foreach(idx => {
            conn.createStatement.execute(
              f"create index ${idx._1} on ${table} ${idx._2.mkString("(", ",", ")")}"
            )
          })
        }
      }

    def assertForeignKeys(
        database: String,
        table: String,
        foreignKeys: ArrayBuffer[
          (String, ArrayBuffer[String], String, ArrayBuffer[String])
        ]
    ): Unit =
      logger.debug("Asserting foreign keys for table {}", table)

      val md = conn.getMetaData
      Using(md.getImportedKeys(database, null, table)) { rs =>
        {
          while (rs.next) do
            foreignKeys.filterInPlace(p => p._1 != rs.getString("FK_NAME"))

          foreignKeys.foreach(fk =>
            conn.createStatement.execute(
              f"alter table ${table} add constraint ${fk._1} foreign key ${fk._2
                .mkString("(", ", ", ")")} references ${fk._3} ${fk._4
                .mkString("(", ",", ")")}"
            )
          )
        }
      }

    def assert: Unit =
      logger.info("Running assert methods")

      val database = props.getProperty("database.name")

      // siteinfo columns/table
      assertColumnTables(database, "siteinfo", siteinfoColumns, null)
      assertIndexes(database, "siteinfo", siteinfoIndexes)

      // namespaces columns/table
      assertColumnTables(
        database,
        "namespaces",
        namespacesColumns,
        namespacesComposite
      )
      assertIndexes(database, "namespaces", namespacesIndexes)

      // page columns/table
      assertColumnTables(database, "page", pageColumns, pageComposite)
      assertIndexes(database, "page", pageIndexes)

      // revision columns/table
      assertColumnTables(
        database,
        "revision",
        revisionColumns,
        revisionComposite
      )
      assertIndexes(database, "revision", revisionIndexes)

      // contributor columns/table
      assertColumnTables(
        database,
        "contributor",
        contributorColumns,
        contributorComposite
      )
      assertIndexes(database, "contributor", contributorIndexes)

      // foreign keys
      assertForeignKeys(database, "contributor", contributorForeignKeys)
      assertForeignKeys(database, "namespaces", namespacesForeignKeys)
      assertForeignKeys(database, "page", pageForeignKeys)
      assertForeignKeys(database, "revision", revisionForeignKeys)

    def close: Unit =
      conn.close

  class SinkSender(props: Properties)
      extends Closeable
      with org.barewiki.sinks.SinkSender:
    val logger = LoggerFactory.getLogger(this.getClass)

    val connPage = Connector.getConnection(props)
    val psPage = connPage.prepareStatement(
      f"insert into page ${pageColumns.map(c => c._1).mkString("(", ", ", ")")} values ${pageColumns
        .map(_ => "?")
        .mkString("(", ", ", ")")}"
    )

    val connRevision = Connector.getConnection(props)
    val psRevision = connRevision.prepareStatement(
      f"insert into revision ${revisionColumns.map(c => c._1).mkString("(", ", ", ")")} values ${revisionColumns
        .map(_ => "?")
        .mkString("(", ", ", ")")}"
    )

    val connContributor = Connector.getConnection(props)
    val psContributor = connContributor.prepareStatement(
      f"insert into contributor ${contributorColumns.map(c => c._1).mkString("(", ", ", ")")} values ${contributorColumns
        .map(_ => "?")
        .mkString("(", ", ", ")")} on conflict (id, dbname) do nothing"
    )

    def sendSiteinfo(siteinfo: HashMap[String, String]): Unit =
      if siteinfo == null || !siteinfo.contains("dbname") || siteinfo(
          "dbname"
        ) == null
      then return

      val connSiteinfo = Connector.getConnection(props)
      val psSiteinfo = connSiteinfo.prepareStatement(
        f"insert into siteinfo ${siteinfoColumns
          .map(c => c._1.mkString("\"", "", "\""))
          .mkString("(", ", ", ")")} values ${siteinfoColumns
          .map(c => "?")
          .mkString("(", ", ", ")")} on conflict (dbname) do nothing"
      )

      psSiteinfo.setString(1, siteinfo.get("dbname").orNull)
      psSiteinfo.setString(2, siteinfo.get("sitename").orNull)
      psSiteinfo.setString(3, siteinfo.get("base").orNull)
      psSiteinfo.setString(4, siteinfo.get("generator").orNull)
      psSiteinfo.setString(5, siteinfo.get("case").orNull)
      psSiteinfo.execute
      connSiteinfo.close

    def sendNamespace(namespace: HashMap[String, String]): Unit =
      if namespace == null || !namespace.contains("key") || !namespace.contains(
          "dbname"
        ) || namespace("key") == null || namespace(
          "dbname"
        ) == null
      then return

      val connNamespaces = Connector.getConnection(props)
      val psNamespaces = connNamespaces.prepareStatement(
        f"insert into namespaces ${namespacesColumns
          .map(c => c._1.mkString("\"", "", "\""))
          .mkString("(", ", ", ")")} values ${namespacesColumns
          .map(c => "?")
          .mkString("(", ", ", ")")} on conflict (key, dbname) do nothing"
      )

      val optKey = namespace.get("key")
      if optKey.isDefined then psNamespaces.setShort(1, optKey.get.toShort)
      else psNamespaces.setNull(1, Types.NULL)

      psNamespaces.setString(2, namespace.get("dbname").orNull)

      psNamespaces.setString(3, namespace.get("case").orNull)
      psNamespaces.setString(4, namespace.get("name").orNull)
      psNamespaces.execute
      connNamespaces.close

    def sendPage(page: HashMap[String, String]): Unit =
      if page == null || !page.contains("id") || !page.contains(
          "dbname"
        ) || page("id") == null || page("dbname") == null
      then return

      psPage.setLong(1, page("id").toLong)
      psPage.setString(2, page.get("dbname").orNull)

      val optNs = page.get("ns")
      val optRevisionId = page.get("revision_id")

      if optNs.isDefined then psPage.setShort(3, optNs.get.toShort)
      else psPage.setShort(3, Types.NULL)

      psPage.setString(4, page.get("title").orNull)
      psPage.setString(5, page.get("redirect").orNull)

      if optRevisionId.isDefined then
        psPage.setLong(6, optRevisionId.get.toLong)
      else psPage.setLong(6, Types.NULL)

      psPage.addBatch

    def sendRevision(revision: HashMap[String, String]): Unit =
      if revision == null || !revision.contains("id") || !revision.contains(
          "dbname"
        ) || revision("id") == null || revision("dbname") == null
      then return

      psRevision.setLong(1, revision("id").toLong)
      psRevision.setString(2, revision.get("dbname").orNull)

      val optParentId = revision.get("parentid")
      val optTimestamp = revision.get("timestamp")
      val optBytes = revision.get("bytes")
      val optContributorId = revision.get("contributor_id")

      if optParentId.isDefined then
        psRevision.setLong(3, optParentId.get.toLong)
      else psRevision.setNull(3, Types.NULL)

      if optTimestamp.isDefined then
        psRevision.setTimestamp(
          4,
          Timestamp(
            dtf
              .parse(optTimestamp.get)
              .getLong(ChronoField.INSTANT_SECONDS) * 1000
          )
        )
      else psRevision.setNull(4, Types.NULL)

      psRevision.setString(5, revision.get("comment").orNull)
      psRevision.setString(6, revision.get("model").orNull)
      psRevision.setString(7, revision.get("format").orNull)
      psRevision.setString(8, revision.get("text").orNull)

      if optBytes.isDefined then psRevision.setLong(9, optBytes.get.toLong)
      else psRevision.setNull(9, Types.NULL)

      psRevision.setString(10, revision.get("sha1").orNull)

      if optContributorId.isDefined then
        psRevision.setLong(11, optContributorId.get.toLong)
      else psRevision.setNull(11, Types.NULL)

      psRevision.addBatch

    def sendContributor(contributor: HashMap[String, String]): Unit =
      if !contributor.contains("id") || !contributor.contains(
          "dbname"
        ) || contributor("id") == null || contributor("dbname") == null
      then return

      psContributor.setLong(1, contributor("id").toLong)
      psContributor.setString(2, contributor.get("dbname").orNull)
      psContributor.setString(3, contributor.get("username").orNull)

      psContributor.addBatch

    private def dispatchRevision: Unit =
      try psRevision.executeBatch
      catch case e => e.getMessage

    private def dispatchPage: Unit =
      try psPage.executeBatch
      catch case e => e.getMessage

    private def dispatchContributor: Unit =
      try psContributor.executeBatch
      catch case e => e.getMessage

    def dispatch: Unit =
      dispatchContributor
      dispatchRevision
      dispatchPage

    def close: Unit =
      dispatch
      connPage.close
      connRevision.close
      connContributor.close

// this one would be amazing to have soon
object Kafka:
  class SinkAssert {}
  class SinkSender {}

// this one might be tricky to implement
object ElasticSearch:
  class SinkAssert {}
  class SinkSender {}
