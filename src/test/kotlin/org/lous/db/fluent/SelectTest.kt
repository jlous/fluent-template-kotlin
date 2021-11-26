package org.lous.db.fluent

import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectTest {

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
        db.sql("INSERT INTO thingy(id, name) VALUES(4,  null) ").execute()
    }

    @Test
    fun `fluent example`() {
        val names = db.sql("""
                SELECT name 
                FROM thingy 
                WHERE name = :name 
                    OR id IN (:ids)
            """)
            .withVarchar("name", "unique")
            .withBigints("ids", 1L, 2L)
            .select.column.asStrings

        assertThat(names).containsExactlyInAnyOrder("unique", "duplicate")
    }

    @Test
    fun `extract works`() {
        val stats = db.sql("SELECT DISTINCT name FROM thingy ORDER BY name DESC").select.extract { rs ->
            rs.next()
            rs.next()
            rs.toString()
        }

        assertThat(stats).contains("rows: 3").contains("pos: 1")
    }

}