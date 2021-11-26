package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectRowsTest {

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
        val multipleRows = db.sql("SELECT id, name FROM thingy WHERE name='duplicate' ORDER BY id ASC")
        val noRows = db.sql("SELECT id, name FROM thingy WHERE name='unknown'")

        assertThat(multipleRows.select.rows.map(getPair)).containsExactly(2L to "duplicate", 3L to "duplicate")
        assertThat(noRows.select.rows.map(getPair)).isEmpty()
    }

    @Test
    fun `asMaps can handle most result shapes`() {
        val allRows = db.sql("SELECT id, name FROM thingy ORDER BY id")
        val noRows = db.sql("SELECT id, name FROM thingy WHERE name='unknown'")

        assertThat(allRows.select.rows.asMaps).hasSize(4).contains(
            mapOf(
                "ID" to 1L,
                "NAME" to "unique"
            )
        )
        assertThat(noRows.select.rows.asMaps).isEmpty()
    }

    @Test
    fun `foreach works`() {
        val names = ArrayList<String>()
        val allRows = db.sql("SELECT name FROM thingy ORDER BY id")

        allRows.select.rows.forEach { rs -> names.add(rs.getString("name")) }

        assertThat(names).contains("unique", "duplicate", "duplicate", null)
    }

}