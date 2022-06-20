/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class TestUtils {

  public static void assertResultSetEqual(
      ResultSet actualResultSet, String expectedHeader, String[] expectedRetArray)
      throws SQLException {
    ResultSetMetaData resultSetMetaData = actualResultSet.getMetaData();
    StringBuilder header = new StringBuilder();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      header.append(resultSetMetaData.getColumnName(i)).append(",");
    }
    assertEquals(expectedHeader, header.toString());

    int cnt = 0;
    while (actualResultSet.next()) {
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        builder.append(actualResultSet.getString(i)).append(",");
      }
      assertEquals(expectedRetArray[cnt], builder.toString());
      cnt++;
    }
    assertEquals(expectedRetArray.length, cnt);
  }
}
