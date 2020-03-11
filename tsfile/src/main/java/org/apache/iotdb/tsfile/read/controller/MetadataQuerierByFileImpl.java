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
package org.apache.iotdb.tsfile.read.controller;

import java.util.Map.Entry;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader.LocateStatus;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import java.io.IOException;
import java.util.*;

public class MetadataQuerierByFileImpl implements IMetadataQuerier {

  private static final int CHUNK_METADATA_CACHE_SIZE = 10000;

  private TsFileMetadata fileMetaData;

  private LRUCache<Path, List<ChunkMetadata>> chunkMetaDataCache;

  private TsFileSequenceReader tsFileReader;

  /**
   * Constructor of MetadataQuerierByFileImpl.
   */
  public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
    this.tsFileReader = tsFileReader;
    this.fileMetaData = tsFileReader.readFileMetadata();
    chunkMetaDataCache = new LRUCache<Path, List<ChunkMetadata>>(CHUNK_METADATA_CACHE_SIZE) {
      @Override
      public List<ChunkMetadata> loadObjectByKey(Path key) throws IOException {
        return loadChunkMetadata(key);
      }
    };
  }

  @Override
  public List<ChunkMetadata> getChunkMetaDataList(Path path) throws IOException {
    return chunkMetaDataCache.get(path);
  }

  @Override
  public Map<Path, List<ChunkMetadata>> getChunkMetaDataMap(List<Path> paths) throws IOException {
    Map<Path, List<ChunkMetadata>> chunkMetaDatas = new HashMap<>();
    for (Path path : paths) {
      if (!chunkMetaDatas.containsKey(path)) {
        chunkMetaDatas.put(path, new ArrayList<>());
      }
      chunkMetaDatas.get(path).addAll(getChunkMetaDataList(path));
    }
    return chunkMetaDatas;
  }

  @Override
  public TsFileMetadata getWholeFileMetadata() {
    return fileMetaData;
  }

  @Override
  public void loadChunkMetaDatas(List<Path> paths) throws IOException {
    // group measurements by device
    TreeMap<String, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      if (!deviceMeasurementsMap.containsKey(path.getDevice())) {
        deviceMeasurementsMap.put(path.getDevice(), new HashSet<>());
      }
      deviceMeasurementsMap.get(path.getDevice()).add(path.getMeasurement());
    }

    Map<Path, List<ChunkMetadata>> tempChunkMetaDatas = new HashMap<>();

    int count = 0;
    boolean enough = false;

    for (Map.Entry<String, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {
      if (enough) {
        break;
      }
      String selectedDevice = deviceMeasurements.getKey();
      // s1, s2, s3
      Set<String> selectedMeasurements = deviceMeasurements.getValue();
      if (fileMetaData.getDeviceMetadataIndex() == null
          || !fileMetaData.getDeviceMetadataIndex().containsKey(selectedDevice)) {
        continue;
      }

      Map<String, TimeseriesMetadata> timeseriesMetaDataInDevice = tsFileReader
          .readDeviceMetadata(selectedDevice);
      List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
      for (Map.Entry<String, TimeseriesMetadata> entry : timeseriesMetaDataInDevice.entrySet()) {
        if (selectedMeasurements.contains(entry.getKey())) {
          chunkMetadataList.addAll(tsFileReader.readChunkMetaDataList(entry.getValue()));
        }
      }
      // d1
      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        String currentMeasurement = chunkMetaData.getMeasurementUid();

        // s1
        if (selectedMeasurements.contains(currentMeasurement)) {

          // d1.s1
          Path path = new Path(selectedDevice, currentMeasurement);

          // add into tempChunkMetaDatas
          if (!tempChunkMetaDatas.containsKey(path)) {
            tempChunkMetaDatas.put(path, new ArrayList<>());
          }
          tempChunkMetaDatas.get(path).add(chunkMetaData);

          // check cache size, stop when reading enough
          count++;
          if (count == CHUNK_METADATA_CACHE_SIZE) {
            enough = true;
            break;
          }
        }
      }
    }

    for (Map.Entry<Path, List<ChunkMetadata>> entry : tempChunkMetaDatas.entrySet()) {
      chunkMetaDataCache.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public TSDataType getDataType(Path path) throws IOException {
    if (tsFileReader.getChunkMetadataList(path) == null || tsFileReader.getChunkMetadataList(path)
        .isEmpty()) {
      return null;
    }
    return tsFileReader.getChunkMetadataList(path).get(0).getDataType();

  }

  private List<ChunkMetadata> loadChunkMetadata(Path path) throws IOException {
    return tsFileReader.getChunkMetadataList(path);
  }


  @Override
  public List<TimeRange> convertSpace2TimePartition(List<Path> paths, long spacePartitionStartPos,
      long spacePartitionEndPos) throws IOException {
    if (spacePartitionStartPos > spacePartitionEndPos) {
      throw new IllegalArgumentException(
          "'spacePartitionStartPos' should not be larger than 'spacePartitionEndPos'.");
    }

    // (1) get timeRangesInCandidates and timeRangesBeforeCandidates by iterating
    // through the metadata
    ArrayList<TimeRange> timeRangesInCandidates = new ArrayList<>();
    ArrayList<TimeRange> timeRangesBeforeCandidates = new ArrayList<>();

    // group measurements by device

    TreeMap<String, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      deviceMeasurementsMap.computeIfAbsent(path.getDevice(), (key) -> new HashSet<>()).add(path.getMeasurement());
    }
    for (Map.Entry<String, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {
      String selectedDevice = deviceMeasurements.getKey();
      Set<String> selectedMeasurements = deviceMeasurements.getValue();

      // measurement -> ChunkMetadata list
      Map<String, List<ChunkMetadata>> seriesMetadatas = tsFileReader
          .readChunkMetadataInDevice(selectedDevice);

      for (Entry<String, List<ChunkMetadata>> seriesMetadata : seriesMetadatas.entrySet()) {

        if (!selectedMeasurements.contains(seriesMetadata.getKey())) {
          continue;
        }

        // time range of this partition of this measurement
        TimeRange inPartitionTimeRange = new TimeRange(Long.MAX_VALUE, Long.MIN_VALUE);
        // time range of the previous partition of this measurement
        TimeRange previousPartitionTimeRange = new TimeRange(Long.MAX_VALUE, Long.MIN_VALUE);

        for (ChunkMetadata chunkMetadata : seriesMetadata.getValue()) {
          LocateStatus location = checkLocateStatus(chunkMetadata, spacePartitionStartPos,
              spacePartitionEndPos);
          if (location == LocateStatus.after) {
            break;
          }

          if (location == LocateStatus.in) {
            // init min time
            if (inPartitionTimeRange.getMin() == Long.MAX_VALUE) {
              inPartitionTimeRange.setMin(chunkMetadata.getStartTime());
            }
            // update max time
            inPartitionTimeRange.setMax(chunkMetadata.getEndTime());
          } else {
            // init min time
            if (previousPartitionTimeRange.getMin() == Long.MAX_VALUE) {
              previousPartitionTimeRange.setMin(chunkMetadata.getStartTime());
            }
            // update max time
            previousPartitionTimeRange.setMax(chunkMetadata.getEndTime());
          }
        }

        if (previousPartitionTimeRange.getMin() != Long.MAX_VALUE) {
          timeRangesBeforeCandidates.add(previousPartitionTimeRange);
        }

        if (inPartitionTimeRange.getMin() != Long.MAX_VALUE) {
          timeRangesInCandidates.add(inPartitionTimeRange);
        }
      }
    }

    // (2) sort and merge the timeRangesInCandidates
    ArrayList<TimeRange> timeRangesIn = new ArrayList<>(
        TimeRange.sortAndMerge(timeRangesInCandidates));
    if (timeRangesIn.isEmpty()) {
      return Collections.emptyList(); // return an empty list
    }

    // (3) sort and merge the timeRangesBeforeCandidates
    ArrayList<TimeRange> timeRangesBefore = new ArrayList<>(
        TimeRange.sortAndMerge(timeRangesBeforeCandidates));

    // (4) calculate the remaining time ranges
    List<TimeRange> resTimeRanges = new ArrayList<>();
    for (TimeRange in : timeRangesIn) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(timeRangesBefore));
      resTimeRanges.addAll(remains);
    }

    return resTimeRanges;
  }


  /**
   * Check the location of a given chunkGroupMetaData with respect to a space partition constraint.
   *
   * @param chunkMetaData          the given chunkMetaData
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos   the end position of the space partition
   * @return LocateStatus
   */


  public static LocateStatus checkLocateStatus(ChunkMetadata chunkMetaData,
      long spacePartitionStartPos, long spacePartitionEndPos) {
    long startOffsetOfChunk = chunkMetaData.getOffsetOfChunkHeader();
    if (spacePartitionStartPos <= startOffsetOfChunk
        && startOffsetOfChunk < spacePartitionEndPos) {
      return LocateStatus.in;
    } else if (startOffsetOfChunk < spacePartitionStartPos) {
      return LocateStatus.before;
    } else {
      return LocateStatus.after;
    }
  }

  @Override
  public void clear() {
    chunkMetaDataCache.clear();
  }

}
