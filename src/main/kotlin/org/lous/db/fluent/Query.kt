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

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

class Query(val spring: NamedParameterJdbcTemplate, val sql: String) {
    val params: TypedParams = TypedParams()

    val select get() = Select(this)
    fun update(): Int = spring.update(sql, params)
    fun execute() = update()

    fun withBigint(name: String, value: Long) = this.apply {
        params.withBigint(name, value)
    }

    fun withBigints(name: String, vararg values: Long) = this.apply {
        params.withBigints(name, values.asList())
    }

    fun withBigints(name: String, values: Collection<Long>) = this.apply {
        params.withBigints(name, values)
    }

    fun withVarchar(name: String, value: String) = this.apply {
        params.withVarchar(name, value)
    }

    fun withVarchars(name: String, values: Collection<String>) = this.apply {
        params.withVarchars(name, values)
    }

    fun withVarchars(name: String, vararg values: String) = this.apply {
        params.withVarchars(name, values.asList())
    }

    fun withClob(name: String, value: String) = this.apply {
        params.withClob(name, value)
    }

    fun withTimestamp(name: String, value: Instant) = this.apply {
        params.withTimestamp(name, value)
    }

    fun withTimestamp(name: String, value: Timestamp) = this.apply {
        params.withTimestamp(name, value)
    }

    fun withTimestamp(name: String, value: LocalDateTime) = withTimestamp(name, value.atZone(ZoneId.systemDefault()).toInstant())

    fun withTimestamp(name: String, value: ZonedDateTime) = withTimestamp(name, value.toInstant())

    fun withTimestamp(name: String, value: OffsetDateTime) = withTimestamp(name, value.toInstant())

}
