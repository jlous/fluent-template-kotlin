package org.lous.db.fluent
/*
 The MIT License (MIT)
 Copyright © 2021 Joachim Lous

 Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 associated documentation files (the “Software”), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be included in all copies or substantial
 portions of the Software.

 THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.SingleColumnRowMapper
import java.sql.ResultSet
import java.time.Instant
import java.util.function.Consumer

class SelectColumn(private val q: Query) {
    /**
     * Expects exactly one column in query result
     *
     * @param elementType must be natively supported by Spring Template
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    fun <T> asListOf(elementType: Class<T>): List<T> {
        return q.spring.queryForList(q.sql, q.params, elementType)
    }

    /**
     * Expects exactly one column in query result
     *
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asStrings get() = asListOf(String::class.java)

    /**
     * Expects exactly one column in query result
     *
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asLongs get() = asListOf(Long::class.java)

    /**
     * Expects exactly one column in query result
     *
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asInstants get() = asListOf(Instant::class.java)

    fun <T> forEach(elementType: Class<T>, consumer: Consumer<T>) {
        val elementMapper = SingleColumnRowMapper(elementType)
        forEach<T>(elementMapper, consumer)
    }

    protected fun <T> forEach(elementMapper: RowMapper<T>, consumer: Consumer<T>) {
        val rowMapper = RowMapper<T> { rs: ResultSet, rowNum: Int ->
            elementMapper.mapRow(rs, rowNum)
                ?.let { consumer.accept(it) }
            null
        }
        q.spring.query(q.sql, q.params, rowMapper)
    }
}
