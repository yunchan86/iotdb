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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SuppressWarnings("squid:S106")
public class SessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    // createTimeseries();

    // insertTablet();

    query();
    session.close();
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      session.createTimeseries(
          ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    //    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    //    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 10000);

    int cnt = 1;
    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 1000000; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      long[] sensor = (long[]) values[0];
      sensor[0] = new Random().nextLong();
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
        System.out.printf("========= %d =========\n", cnt);
        cnt++;
      }
    }

    for (long time = 5000000; time < 10000000; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      long[] sensor = (long[]) values[0];
      sensor[0] = new Random().nextLong();
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
        System.out.printf("========= %d =========\n", cnt);
        cnt++;
      }
    }

    for (long time = 1000000; time < 5000000; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      long[] sensor = (long[]) values[0];
      sensor[0] = new Random().nextLong();
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
        System.out.printf("========= %d =========\n", cnt);
        cnt++;
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    long startTime = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      // session.executeQueryStatement("select count(s1) from root.sg1.d1");
      session.executeQueryStatement(
          "select count(s1) from root.sg1.d1 group by ([5, 4523623), 511ms)");
    }
    // 旧 Time: 125.357620 ms Time: 124.753079 ms
    // 新 Time: 167.694330 ms
    System.out.printf("Time: %f ms", (System.nanoTime() - startTime) / 1e8);
  }
}
