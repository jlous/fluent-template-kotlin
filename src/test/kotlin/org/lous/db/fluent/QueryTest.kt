package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

@TestInstance(PER_CLASS)
class QueryTest {

    private lateinit var db: FluentTemplate

    @BeforeAll
    fun initSchema() {
        db = FluentTemplate(JdbcConnectionPool.create("jdbc:h2:mem:queryparamtest", "test", "test"))
        db.sql("DROP TABLE IF EXISTS thingy").execute()
        db.sql("""
            CREATE TABLE thingy (
                id BIGINT,     
                name VARCHAR(100),     
                type VARCHAR(100),     
                document CLOB,     
                created_utc TIMESTAMP
            )
        """).execute()
    }

    @BeforeEach
    fun resetBase() {
        db.sql("TRUNCATE TABLE thingy").execute()
    }

    @Test
    fun `maps single params`() {
        val instant: Instant = Instant.now()
        val queryWithSingleParams = db.sql("INSERT INTO thingy VALUES(:id, :name, :type, :doc, :created)")
            .withBigint("id", 5L)
            .withVarchar("name", "Ulysses")
            .withVarchar("type", "novel")
            .withClob("doc", "Lotsa text")
            .withTimestamp("created", instant)

        queryWithSingleParams.execute()

        assertThat(db.sql("SELECT * FROM thingy").select.row.asMap)
            .containsValues(5L, "Ulysses", "novel", "Lotsa text", Timestamp.from(instant))
    }

    @Test
    fun `maps multiple varchars`() {
        val insertName = "INSERT INTO thingy (id, name) VALUES(:id, :name)"
        db.sql(insertName).withBigint("id", 1L).withVarchar("name", "per").execute()
        db.sql(insertName).withBigint("id", 2L).withVarchar("name", "ola").execute()
        db.sql(insertName).withBigint("id", 3L).withVarchar("name", "ramses").execute()

        val fetchedByList = db.sql("SELECT * FROM thingy WHERE name IN (:names)")
            .withVarchars("names", listOf("per", "ramses"))
            .select.rows.asMaps
        val fetchedByVarargs = db.sql("SELECT * FROM thingy WHERE name IN (:names)")
            .withVarchars("names", "per", "ramses")
            .select.rows.asMaps

        assertThat(fetchedByList).hasSize(2)
        assertThat(fetchedByVarargs).hasSize(2)
    }

    @Test
    fun `maps multiple bigints`() {
        val insertName = "INSERT INTO thingy (id, name) VALUES(:id, :name)"
        db.sql(insertName).withBigint("id", 1L).withVarchar("name", "per").execute()
        db.sql(insertName).withBigint("id", 2L).withVarchar("name", "ola").execute()
        db.sql(insertName).withBigint("id", 3L).withVarchar("name", "ramses").execute()

        val fetchedWithList = db.sql("SELECT * FROM thingy WHERE id IN (:ids)")
            .withBigints("ids", listOf(1L, 3L))
            .select.rows.asMaps
        val fetchedWithVarargs = db.sql("SELECT * FROM thingy WHERE id IN (:ids)")
            .withBigints("ids", 1L, 3L)
            .select.rows.asMaps

        assertThat(fetchedWithList).hasSize(2)
        assertThat(fetchedWithVarargs).hasSize(2)
    }

    @Test
    fun `maps alternate time formats equally`() {
        val instant: Instant = Instant.now()
        val insertCreated = "INSERT INTO thingy (created_utc) VALUES(:created)"
        val withInstant =
            db.sql(insertCreated).withTimestamp("created", instant)
        val insertWithTimestamp =
            db.sql(insertCreated).withTimestamp("created", Timestamp.from(instant))
        val withOffsetDateTimeTimestamp =
            db.sql(insertCreated).withTimestamp("created", OffsetDateTime.ofInstant(instant, ZoneId.systemDefault()))
        val withZonedDateTime =
            db.sql(insertCreated).withTimestamp("created", ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()))
        val withLocalDateTime =
            db.sql(insertCreated).withTimestamp("created", LocalDateTime.ofInstant(instant, ZoneId.systemDefault()))

        withInstant.execute()
        insertWithTimestamp.execute()
        withOffsetDateTimeTimestamp.execute()
        withZonedDateTime.execute()
        withLocalDateTime.execute()

        assertThat(db.sql("SELECT created_utc FROM thingy").select.column.asInstants)
            .hasSize(5)
            .containsOnly(instant)
    }

    @Test
    fun `update counts inserted rows`() {
        db.sql("INSERT INTO thingy (id, name) VALUES(1, 'fat')").execute()
        db.sql("INSERT INTO thingy (id, name) VALUES(2, 'protein')").execute()
        db.sql("INSERT INTO thingy (id, name) VALUES(3, 'carbs')").execute()

        val count = db.sql("INSERT INTO thingy (id, name) VALUES(4, 'bacon')").update()

        assertThat(count).isEqualTo(1)
    }

    @Test
    fun `update counts updated rows`() {
        db.sql("INSERT INTO thingy (id, name) VALUES(1, 'fat')").execute()
        db.sql("INSERT INTO thingy (id, name) VALUES(2, 'protein')").execute()
        db.sql("INSERT INTO thingy (id, name) VALUES(3, 'carbs')").execute()
        db.sql("INSERT INTO thingy (id, name) VALUES(4, 'bacon')").execute()

        val count = db.sql("UPDATE thingy SET name='bacon' WHERE name != 'bacon'").update()

        assertThat(count).isEqualTo(3)
    }
}
