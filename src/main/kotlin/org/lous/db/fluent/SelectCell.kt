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

import org.springframework.dao.IncorrectResultSizeDataAccessException
import java.time.Instant

class SelectCell(private val q: Query) {
    /**
     * Expects query to return exactly one row with exactly one column with a non-null value
     *
     * @param elementType must be natively supported by Spring Template
     * @throws IncorrectResultSizeDataAccessException for row counts other than one
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     * @throws NullPointerException if cell contains null-value
     */
    fun <T> asSingle(elementType: Class<T>): T {
        return q.spring.queryForObject(q.sql, q.params, elementType)!!
    }

    /**
     * Expects query to return exactly one row with exactly one column
     *
     * @param elementType must be natively supported by Spring Template
     * @return null if cell contains null
     * @throws IncorrectResultSizeDataAccessException for row counts other than one
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    fun <T> asNullable(elementType: Class<T>): T? {
        val hits = q.spring.queryForList(q.sql, q.params, elementType)
        return when (hits.size) {
            1 -> hits[0]
            else -> throw IncorrectResultSizeDataAccessException(1, hits.size)
        }
    }

    /**
     * Expects query to return at most one row with exactly one column
     */
    //TODO: fun ifPresent()

    /**
     * Expects exactly one column in query result with exactly one column
     *
     * @throws IncorrectResultSizeDataAccessException for row counts other than one
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asString get() = asSingle(String::class.java)

    /**
     * Expects exactly one column in query result with exactly one column
     *
     * @throws IncorrectResultSizeDataAccessException for row counts other than one
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asLong get() = asSingle(Long::class.java)

    /**
     * Expects exactly one column in query result with exactly one column
     *
     * @throws IncorrectResultSizeDataAccessException for row counts other than one
     * @throws org.springframework.jdbc.IncorrectResultSetColumnCountException for column counts other than one
     */
    val asInstant get() = asSingle(Instant::class.java)
}
