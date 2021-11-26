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

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import java.sql.JDBCType
import java.sql.Timestamp
import java.time.Instant

class TypedParams : MapSqlParameterSource() {
    fun withBigint(name: String, value: Long): TypedParams = this.apply {
        addValue(name, value, JDBCType.BIGINT.vendorTypeNumber)
    }

    fun withBigints(name: String, values: Collection<Long>): TypedParams = this.apply {
        addValue(name, values, JDBCType.BIGINT.vendorTypeNumber)
    }

    fun withVarchar(name: String, value: String): TypedParams = this.apply {
        addValue(name, value, JDBCType.VARCHAR.vendorTypeNumber)
    }

    fun withVarchars(name: String, values: Collection<String>): TypedParams = this.apply {
        addValue(name, values, JDBCType.VARCHAR.vendorTypeNumber)
    }

    fun withClob(name: String, value: String): TypedParams = this.apply {
        addValue(name, value, JDBCType.CLOB.vendorTypeNumber)
    }

    fun withTimestamp(name: String, value: Instant): TypedParams = this.apply {
        withTimestamp(name, Timestamp.from(value))
    }

    fun withTimestamp(name: String, value: Timestamp): TypedParams = this.apply {
        addValue(name, value, JDBCType.TIMESTAMP.vendorTypeNumber)
    }
}
