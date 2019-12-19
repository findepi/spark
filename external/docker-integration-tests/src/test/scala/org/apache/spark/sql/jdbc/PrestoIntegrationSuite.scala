/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc

import java.lang.String.join
import java.sql.Connection
import java.util.Collections.nCopies
import java.util.Properties

@DockerTest
class PrestoIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = "prestosql/presto:326"
    override val env = Map()
    override val usesIpc = false
    override val jdbcPort = 8080
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:presto://$ip:$port/memory/default"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE bar AS SELECT " +
      "true c_boolean," +
      "TINYINT '13' c_tinyint," +
      "SMALLINT '1234' c_smallint," +
      "123456 c_integer," +
      "123456789123456789 c_bigint, " +
      "REAL '1.25' c_real, " +
      "42.42e0 c_double, " +
      "CAST('abc' AS varchar) c_varchar, " +
      "ARRAY[BIGINT '42', BIGINT '53'] c_array")
      .executeUpdate()

    conn.prepareStatement("INSERT INTO bar VALUES (" + join(", ", nCopies(9, "NULL")) + ")")
      .executeUpdate()
  }

  test("Type mapping for various types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    val rows = df.collect().sortBy(_.toString())
    assert(rows.length == 2)

    // Test the types, and values using the first row.
    val types = rows(0).toSeq.map(x => x.getClass)
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(0)))
    assert(classOf[java.lang.Byte].isAssignableFrom(types(1)))
    assert(classOf[java.lang.Short].isAssignableFrom(types(2)))
    assert(classOf[java.lang.Integer].isAssignableFrom(types(3)))
    assert(classOf[java.lang.Long].isAssignableFrom(types(4)))
    assert(classOf[java.lang.Float].isAssignableFrom(types(5)))
    assert(classOf[java.lang.Double].isAssignableFrom(types(6)))
    assert(classOf[String].isAssignableFrom(types(7)))
    assert(classOf[Array[Long]].isAssignableFrom(types(8)))

    assert(rows(0).getBoolean(0) == true)
    assert(rows(0).getByte(1) == 13)
    assert(rows(0).getShort(2) == 1334)
    assert(rows(0).getInt(3) == 123456)
    assert(rows(0).getLong(4) == 123456789123456789L)
    assert(rows(0).getFloat(5) == 1.25)
    assert(rows(0).getDouble(6) == 42.42)
    assert(rows(0).getString(7).equals("abc"))
    assert(rows(0).getSeq(8).toSeq == Seq(42L, 53L))

    // Test reading null values using the second row.
    assert(0.until(types.length).forall(rows(1).isNullAt(_)))
  }

  test("Basic write test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    // Test only that it doesn't crash.
    df.write.jdbc(jdbcUrl, "barcopy", new Properties)
    // Test that written numeric type has same DataType as input
    assert(sqlContext.read.jdbc(jdbcUrl, "barcopy", new Properties).schema(13).dataType ==
      ArrayType(DecimalType(2, 2), true))
    // Test write null values.
    df.select(df.queryExecution.analyzed.output.map { a =>
      Column(Literal.create(null, a.dataType)).as(a.name)
    }: _*).write.jdbc(jdbcUrl, "barcopy2", new Properties)
  }

  test("Creating a table with shorts and floats") {
    sqlContext.createDataFrame(Seq((1.0f, 1.toShort)))
      .write.jdbc(jdbcUrl, "shortfloat", new Properties)
    val schema = sqlContext.read.jdbc(jdbcUrl, "shortfloat", new Properties).schema
    assert(schema(0).dataType == FloatType)
    assert(schema(1).dataType == ShortType)
  }

  test("SPARK-20557: column type TIMESTAMP with TIME ZONE and TIME with TIME ZONE " +
    "should be recognized") {
    // When using JDBC to read the columns of TIMESTAMP with TIME ZONE and TIME with TIME ZONE
    // the actual types are java.sql.Types.TIMESTAMP and java.sql.Types.TIME
    val dfRead = sqlContext.read.jdbc(jdbcUrl, "ts_with_timezone", new Properties)
    val rows = dfRead.collect()
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(1).equals("class java.sql.Timestamp"))
    assert(types(2).equals("class java.sql.Timestamp"))
  }

  test("SPARK-22291: Conversion error when transforming array types of " +
    "uuid, inet and cidr to StingType in PostgreSQL") {
    val df = sqlContext.read.jdbc(jdbcUrl, "st_with_array", new Properties)
    val rows = df.collect()
    assert(rows(0).getString(0) == "0a532531-cdf1-45e3-963d-5de90b6a30f1")
    assert(rows(0).getString(1) == "172.168.22.1")
    assert(rows(0).getString(2) == "192.168.100.128/25")
    assert(rows(0).getString(3) == "{\"a\": \"foo\", \"b\": \"bar\"}")
    assert(rows(0).getString(4) == "{\"a\": 1, \"b\": 2}")
    assert(rows(0).getSeq(5) == Seq("7be8aaf8-650e-4dbb-8186-0a749840ecf2",
      "205f9bfc-018c-4452-a605-609c0cfad228"))
    assert(rows(0).getSeq(6) == Seq("172.16.0.41", "172.16.0.42"))
    assert(rows(0).getSeq(7) == Seq("192.168.0.0/24", "10.1.0.0/16"))
    assert(rows(0).getSeq(8) == Seq("""{"a": "foo", "b": "bar"}""", """{"a": 1, "b": 2}"""))
    assert(rows(0).getSeq(9) == Seq("""{"a": 1, "b": 2, "c": 3}"""))
  }

  test("query JDBC option") {
    val expectedResult = Set(
      (42, 123456789012345L)
    ).map { case (c1, c3) =>
      Row(Integer.valueOf(c1), java.lang.Long.valueOf(c3))
    }

    val query = "SELECT c1, c3 FROM bar WHERE c1 IS NOT NULL"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", query)
      .load()
    assert(df.collect.toSet === expectedResult)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl', query '$query')
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select c1, c3 from queryOption").collect.toSet == expectedResult)
  }

  test("write byte as smallint") {
    sqlContext.createDataFrame(Seq((1.toByte, 2.toShort)))
      .write.jdbc(jdbcUrl, "byte_to_smallint_test", new Properties)
    val df = sqlContext.read.jdbc(jdbcUrl, "byte_to_smallint_test", new Properties)
    val schema = df.schema
    assert(schema.head.dataType == ShortType)
    assert(schema(1).dataType == ShortType)
    val rows = df.collect()
    assert(rows.length === 1)
    assert(rows(0).getShort(0) === 1)
    assert(rows(0).getShort(1) === 2)
  }
}
