package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectFirstRowTest {

    private lateinit var db: FluentTemplate
    private val getPair = RowMapper<Pair<Long, String>> { rs: ResultSet, _: Int -> rs.getLong("id") to rs.getString("name") }

    @BeforeAll
    fun initSchema() {
        db = FluentTemplate(JdbcConnectionPool.create("jdbc:h2:mem:queryparamtest", "test", "test"))
        db.sql("DROP TABLE IF EXISTS thingy").execute()
        db.sql("""
            CREATE TABLE thingy (
                id BIGINT,
                name VARCHAR(100)
            )
        """).execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(1, 'unique')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(2, 'duplicate')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(3, 'duplicate')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(4,  null) ").execute()
    }

    @Test
    fun `map can handle most result shapes`() {
        val perfectMatch = db.sql("SELECT id, name FROM thingy WHERE name='unique'")
        val extraWidth = db.sql("SELECT id, name, 'extraColumn' FROM thingy WHERE name='unique'")
        val extraHeight = db.sql("SELECT id, name FROM thingy WHERE name='duplicate' ORDER BY id DESC")
        val noHit = db.sql("SELECT id, name FROM thingy WHERE name='unknown'")
        val emptyName = db.sql("SELECT id, name FROM thingy WHERE name IS NULL")

        assertThat(perfectMatch.select.firstRow.map(getPair)).isEqualTo(1L to "unique")
        assertThat(extraWidth.select.firstRow.map(getPair)).isEqualTo(1L to "unique")
        assertThat(extraHeight.select.firstRow.map(getPair)).isEqualTo(3L to "duplicate")
        assertThat(emptyName.select.firstRow.map(getPair)).isEqualTo(4L to null)
        assertThat(noHit.select.firstRow.map(getPair)).isNull()
    }

    @Test
    fun `asMap can handle most result shapes`() {
        val perfectMatch = db.sql("SELECT id, name FROM thingy WHERE name='unique'")
        val extraWidth = db.sql("SELECT id, name, 'extraColumn' FROM thingy WHERE name='unique'")
        val extraHeight = db.sql("SELECT id, name FROM thingy WHERE name='duplicate' ORDER BY id DESC")
        val emptyName = db.sql("SELECT id, name FROM thingy WHERE name IS NULL")
        val noHit = db.sql("SELECT id, name FROM thingy WHERE name='unknown'")

        assertThat(perfectMatch.select.firstRow.asMap).isEqualTo(mapOf(
            "ID" to 1L,
            "NAME" to "unique")
        )
        assertThat(extraWidth.select.firstRow.asMap).isEqualTo(mapOf(
            "ID" to 1L,
            "NAME" to "unique",
            "'extraColumn'" to "extraColumn"
        ))
        assertThat(extraHeight.select.firstRow.asMap).isEqualTo(mapOf(
            "ID" to 3L,
            "NAME" to "duplicate"))
        assertThat(emptyName.select.firstRow.asMap).isEqualTo(mapOf(
            "ID" to 4L,
            "NAME" to null)
        )
        assertThat(noHit.select.firstRow.asMap).isNull()
    }

}