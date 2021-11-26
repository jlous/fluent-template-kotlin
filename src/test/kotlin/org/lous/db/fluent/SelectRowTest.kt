package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.dao.IncorrectResultSizeDataAccessException
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectRowTest {

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
    fun `asMap maps exactly one row`() {
        val allRows = db.sql("SELECT id, name FROM thingy")
        val oneRow = db.sql("SELECT id, name FROM thingy WHERE id=1")
        val noRows = db.sql("SELECT id, name FROM thingy WHERE id=1000")

        assertThat(oneRow.select.row.asMap).isEqualTo(mapOf(
            "ID" to 1L,
            "NAME" to "unique"
        ))
        assertThatThrownBy { allRows.select.row.asMap }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { noRows.select.row.asMap }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
    }

    @Test
    fun `map maps exactly one row`() {
        val allRows = db.sql("SELECT id, name FROM thingy")
        val oneRow = db.sql("SELECT id, name FROM thingy WHERE id=1")
        val noRows = db.sql("SELECT id, name FROM thingy WHERE id=1000")

        assertThat(oneRow.select.row.map(getPair)).isEqualTo(Pair(1L, "unique"))
        assertThatThrownBy { allRows.select.row.map(getPair) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { noRows.select.row.map(getPair) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
    }

}