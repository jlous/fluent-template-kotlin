package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.dao.IncorrectResultSizeDataAccessException
import org.springframework.jdbc.IncorrectResultSetColumnCountException
import java.time.Instant

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectCellTest {

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
        db.sql("INSERT INTO thingy(id, name) VALUES(1, 'unique name')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(2, 'duplicate name')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(3, 'duplicate name')").execute()
        db.sql("INSERT INTO thingy(id, name) VALUES(4,  null) ").execute()
    }

    @Test
    fun `asSingle requires exactly one non-null column from exactly one row`() {
        val oneCell = db.sql("SELECT name FROM thingy WHERE name='unique name'")
        val emptyCell = db.sql("SELECT name FROM thingy WHERE name IS NULL")
        val noHit = db.sql("SELECT name FROM thingy WHERE name='unknown name'")
        val tooTall = db.sql("SELECT name FROM thingy WHERE name='duplicate name'")
        val tooWide = db.sql("SELECT name, id FROM thingy WHERE name='unique name'")

        assertThat(oneCell.select.cell.asSingle(String::class.java)).isEqualTo("unique name")
        assertThatThrownBy { emptyCell.select.cell.asSingle(String::class.java) }.isInstanceOf(NullPointerException::class.java)
        assertThatThrownBy { noHit.select.cell.asSingle(String::class.java) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { tooTall.select.cell.asSingle(String::class.java) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { tooWide.select.cell.asSingle(String::class.java) }.isInstanceOf(IncorrectResultSetColumnCountException::class.java)
    }

    @Test
    fun `asNullable requires exactly one column from exactly one row`() {
        val cell = db.sql("SELECT name FROM thingy WHERE name='unique name'")
        val emptyCell = db.sql("SELECT name FROM thingy WHERE name IS NULL")
        val noHit = db.sql("SELECT name FROM thingy WHERE name='unknown name'")
        val tooTall = db.sql("SELECT name FROM thingy WHERE name='duplicate name'")
        val tooWide = db.sql("SELECT name, id FROM thingy WHERE name='unique name'")

        assertThat(cell.select.cell.asNullable(String::class.java)).isEqualTo("unique name")
        assertThat(emptyCell.select.cell.asNullable(String::class.java)).isNull()
        assertThatThrownBy { noHit.select.cell.asNullable(String::class.java) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { tooTall.select.cell.asNullable(String::class.java) }.isInstanceOf(IncorrectResultSizeDataAccessException::class.java)
        assertThatThrownBy { tooWide.select.cell.asNullable(String::class.java) }.isInstanceOf(IncorrectResultSetColumnCountException::class.java)
    }

    //TODO: cell.ifPresent

    @Test
    fun `shorthand properties work`() {
        db.sql("DROP TABLE IF EXISTS wotsit").execute()
        db.sql("""
            CREATE TABLE wotsit (     
                id BIGINT,     
                name VARCHAR(100),     
                type VARCHAR(100),     
                document CLOB,     
                oppdatert_utc TIMESTAMP
            )
        """).execute()
        db.sql("INSERT INTO wotsit VALUES (1, 'a name', 'a type', 'a document', CURRENT_TIMESTAMP)").execute()
        db.sql("INSERT INTO wotsit VALUES (2, 'a different name', 'a different type', 'a different document', CURRENT_TIMESTAMP)").execute()

        assertThat(db.sql("SELECT id FROM wotsit WHERE id=1").select.cell.asLong).isEqualTo(1)
        assertThat(db.sql("SELECT name FROM wotsit WHERE id=1").select.cell.asString).isEqualTo("a name")
        assertThat(db.sql("SELECT oppdatert_utc FROM wotsit WHERE id=1").select.cell.asInstant).isBeforeOrEqualTo(Instant.now())
    }

}