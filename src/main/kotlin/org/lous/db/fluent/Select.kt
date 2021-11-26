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

import org.springframework.jdbc.core.ResultSetExtractor

class Select(private val query: Query) {
    /**
     * Expects query to return exactly one row with exactly one column
     */
    val cell get() = SelectCell(query)

    /**
     * Expects exactly one column in query result
     */
    val column get() = SelectColumn(query)

    /**
     * Expects exactly one row in query result
     */
    val row get() = SelectRow(query)

    val firstRow get() = SelectFirstRow(query)

    val rows get() = SelectRows(query)

    fun <T> extract(extractor: ResultSetExtractor<T>): T? {
        return query.spring.query(query.sql, query.params, extractor)
    }
}
