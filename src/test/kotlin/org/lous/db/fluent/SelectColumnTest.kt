package org.lous.db.fluent

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.IncorrectResultSetColumnCountException

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectColumnTest {

    private lateinit var db: FluentTemplate

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
        db.sql("INSERT INTO thingy(id, name) VALUES(4, null)").execute()
    }

    @Test
    fun `asListOf requires exactly one column`() {
        val singleCell = db.sql("SELECT name FROM thingy WHERE name='unique'")
        val column = db.sql("SELECT name FROM thingy WHERE name='duplicate'")
        val noHit = db.sql("SELECT name FROM thingy WHERE name='unknown'")
        val tooWide = db.sql("SELECT name, id FROM thingy WHERE name='unique'")

        assertThat(singleCell.select.column.asListOf(String::class.java)).containsExactly("unique")
        assertThat(column.select.column.asListOf(String::class.java)).containsExactly("duplicate", "duplicate")
        assertThat(noHit.select.column.asListOf(String::class.java)).isEmpty()
        Assertions.assertThatThrownBy { tooWide.select.column.asListOf(String::class.java) }.isInstanceOf(IncorrectResultSetColumnCountException::class.java)
    }

    @Test
    fun `shorthand properties work`() {
        db.sql("DROP TABLE IF EXISTS wotsit").execute()
        db.sql(""
                + "CREATE TABLE wotsit ("
                + "     id BIGINT,"
                + "     name VARCHAR(100),"
                + "     type VARCHAR(100),"
                + "     document CLOB,"
                + "     oppdatert_utc TIMESTAMP"
                + ")"
        ).execute()
        db.sql("INSERT INTO wotsit VALUES (1, 'a name', 'a type', 'a document', CURRENT_TIMESTAMP)").execute()
        db.sql("INSERT INTO wotsit VALUES (2, 'a different name', 'a different type', 'a different document', CURRENT_TIMESTAMP)").execute()

        assertThat(db.sql("SELECT name FROM wotsit").select.column.asStrings).containsExactly("a name", "a different name")
        assertThat(db.sql("SELECT id FROM wotsit").select.column.asLongs).containsExactly(1L, 2L)
        assertThat(db.sql("SELECT oppdatert_utc FROM wotsit").select.column.asInstants).hasSize(2)
    }

    @Test
    fun `forEach includes duplicates and nulls`() {
        val allNames = db.sql("SELECT name FROM thingy")
        val foundNames = ArrayList<String?>()

        allNames.select.column.forEach(String::class.java) { name ->
            foundNames.add(name)
        }

        assertThat(foundNames).containsExactly("unique", "duplicate", "duplicate", null)
    }

}