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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TRegionCache;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionLocationsReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.RegionLocationsResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.executor.ConfigRequestExecutor;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionCacheResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements Manager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  /** Manage PartitionTable read/write requests through the ConsensusLayer */
  private final ConsensusManager consensusManager;

  /** Manage cluster node */
  private final NodeManager nodeManager;

  /** Manage cluster schema */
  private final ClusterSchemaManager clusterSchemaManager;

  /** Manage cluster regions and partitions */
  private final PartitionManager partitionManager;

  /** Manage cluster authorization */
  private final PermissionManager permissionManager;

  private final LoadManager loadManager;

  /** Manage procedure */
  private final ProcedureManager procedureManager;

  /** UDF */
  private final UDFManager udfManager;

  public ConfigManager() throws IOException {
    // Build the persistence module
    NodeInfo nodeInfo = new NodeInfo();
    ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    PartitionInfo partitionInfo = new PartitionInfo();
    AuthorInfo authorInfo = new AuthorInfo();
    ProcedureInfo procedureInfo = new ProcedureInfo();
    UDFInfo udfInfo = new UDFInfo();

    // Build state machine and executor
    ConfigRequestExecutor executor =
        new ConfigRequestExecutor(
            nodeInfo, clusterSchemaInfo, partitionInfo, authorInfo, procedureInfo, udfInfo);
    PartitionRegionStateMachine stateMachine = new PartitionRegionStateMachine(this, executor);

    // Build the manager module
    this.nodeManager = new NodeManager(this, nodeInfo);
    this.clusterSchemaManager = new ClusterSchemaManager(this, clusterSchemaInfo);
    this.partitionManager = new PartitionManager(this, partitionInfo);
    this.permissionManager = new PermissionManager(this, authorInfo);
    this.procedureManager = new ProcedureManager(this, procedureInfo);
    this.udfManager = new UDFManager(this, udfInfo);
    this.loadManager = new LoadManager(this);
    this.consensusManager = new ConsensusManager(stateMachine);

    // We are on testing.......
    if (ConfigNodeDescriptor.getInstance().getConf().isEnableHeartbeat()) {
      // Start asking for heartbeat
      new Thread(this.loadManager).start();
    }
  }

  public void close() throws IOException {
    consensusManager.close();
    partitionManager.getRegionCleaner().shutdown();
    procedureManager.shiftExecutor(false);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public DataSet registerDataNode(RegisterDataNodeReq registerDataNodeReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.registerDataNode(registerDataNodeReq);
    } else {
      DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
      dataSet.setStatus(status);
      dataSet.setConfigNodeList(nodeManager.getOnlineConfigNodes());
      return dataSet;
    }
  }

  @Override
  public DataSet getDataNodeInfo(GetDataNodeInfoReq getDataNodeInfoReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.getDataNodeInfo(getDataNodeInfoReq);
    } else {
      DataNodeInfosResp dataSet = new DataNodeInfosResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setTTL(SetTTLReq setTTLReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTTL(setTTLReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorReq setSchemaReplicationFactorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setSchemaReplicationFactor(setSchemaReplicationFactorReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorReq setDataReplicationFactorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setDataReplicationFactor(setDataReplicationFactorReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalReq setTimePartitionIntervalReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTimePartitionInterval(setTimePartitionIntervalReq);
    } else {
      return status;
    }
  }

  @Override
  public DataSet countMatchedStorageGroups(CountStorageGroupReq countStorageGroupReq) {
    TSStatus status = confirmLeader();
    CountStorageGroupResp result = new CountStorageGroupResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.countMatchedStorageGroups(countStorageGroupReq);
    } else {
      result.setStatus(status);
    }
    return result;
  }

  @Override
  public DataSet getMatchedStorageGroupSchemas(GetStorageGroupReq getStorageGroupReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getMatchedStorageGroupSchema(getStorageGroupReq);
    } else {
      StorageGroupSchemaResp dataSet = new StorageGroupSchemaResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setStorageGroup(setStorageGroupReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus deleteStorageGroups(List<String> deletedPaths) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // remove wild
      Map<String, TStorageGroupSchema> deleteStorageSchemaMap =
          getClusterSchemaManager().getMatchedStorageGroupSchemasByName(deletedPaths);
      ArrayList<TStorageGroupSchema> parsedDeleteStorageGroups =
          new ArrayList<>(deleteStorageSchemaMap.values());
      return procedureManager.deleteStorageGroups(parsedDeleteStorageGroups);
    } else {
      return status;
    }
  }

  private List<TSeriesPartitionSlot> calculateRelatedSlot(
      PartialPath path, PartialPath storageGroup) {
    // The path contains `**`
    if (path.getFullPath().contains(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    // path doesn't contain * so the size of innerPathList should be 1
    PartialPath innerPath = path.alterPrefixPath(storageGroup).get(0);
    // The innerPath contains `*` and the only `*` is not in last level
    if (innerPath.getDevice().contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    return Collections.singletonList(
        getPartitionManager().getSeriesPartitionSlot(innerPath.getDevice()));
  }

  @Override
  public TSchemaPartitionResp getSchemaPartition(PathPatternTree patternTree) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      GetSchemaPartitionReq getSchemaPartitionReq = new GetSchemaPartitionReq();
      Map<String, Set<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
      List<PartialPath> relatedPaths = patternTree.getAllPathPatterns();
      List<String> allStorageGroups = getClusterSchemaManager().getStorageGroupNames();
      Map<String, Boolean> scanAllRegions = new HashMap<>();
      for (PartialPath path : relatedPaths) {
        for (String storageGroup : allStorageGroups) {
          try {
            PartialPath storageGroupPath = new PartialPath(storageGroup);
            if (path.overlapWith(storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD))
                && !scanAllRegions.containsKey(storageGroup)) {
              List<TSeriesPartitionSlot> relatedSlot = calculateRelatedSlot(path, storageGroupPath);
              if (relatedSlot.isEmpty()) {
                scanAllRegions.put(storageGroup, true);
                partitionSlotsMap.put(storageGroup, new HashSet<>());
              } else {
                partitionSlotsMap
                    .computeIfAbsent(storageGroup, k -> new HashSet<>())
                    .addAll(relatedSlot);
              }
            }
          } catch (IllegalPathException e) {
            // this line won't be reached in general
            throw new RuntimeException(e);
          }
        }
      }

      // return empty partition
      if (partitionSlotsMap.isEmpty()) {
        TSchemaPartitionResp resp = new TSchemaPartitionResp();
        resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
        resp.setSchemaRegionMap(new HashMap<>());
        return resp;
      }

      getSchemaPartitionReq.setPartitionSlotsMap(
          partitionSlotsMap.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))));

      SchemaPartitionResp resp =
          (SchemaPartitionResp) partitionManager.getSchemaPartition(getSchemaPartitionReq);
      TSchemaPartitionResp result =
          resp.convertToRpcSchemaPartitionResp(getLoadManager().genRealTimeRoutingPolicy());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "GetSchemaPartition receive paths: {}, return TSchemaPartitionResp: {}",
          relatedPaths,
          result);

      return result;
    } else {
      return new TSchemaPartitionResp().setStatus(status);
    }
  }

  @Override
  public TRegionCacheResp getRegionCache() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      TRegionCacheResp result = new TRegionCacheResp();
      TRegionCache regionCache = new TRegionCache();
      regionCache.setTimestamp(System.currentTimeMillis());
      regionCache.setRegionReplicaMap(loadManager.genRealTimeRoutingPolicy());
      result.setStatus(status);
      result.setRegionCache(regionCache);
      return result;
    } else {
      return new TRegionCacheResp().setStatus(status);
    }
  }

  @Override
  public TSchemaPartitionResp getOrCreateSchemaPartition(PathPatternTree patternTree) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> devicePaths = patternTree.getAllDevicePatterns();
      List<String> storageGroups = getClusterSchemaManager().getStorageGroupNames();

      GetOrCreateSchemaPartitionReq getOrCreateSchemaPartitionReq =
          new GetOrCreateSchemaPartitionReq();
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
      for (String devicePath : devicePaths) {
        if (!devicePath.contains("*")) {
          // Only check devicePaths that without "*"
          for (String storageGroup : storageGroups) {
            if (PathUtils.isStartWith(devicePath, storageGroup)) {
              partitionSlotsMap
                  .computeIfAbsent(storageGroup, key -> new ArrayList<>())
                  .add(getPartitionManager().getSeriesPartitionSlot(devicePath));
              break;
            }
          }
        }
      }
      getOrCreateSchemaPartitionReq.setPartitionSlotsMap(partitionSlotsMap);

      SchemaPartitionResp resp =
          (SchemaPartitionResp)
              partitionManager.getOrCreateSchemaPartition(getOrCreateSchemaPartitionReq);
      TSchemaPartitionResp result =
          resp.convertToRpcSchemaPartitionResp(getLoadManager().genRealTimeRoutingPolicy());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "GetOrCreateSchemaPartition receive devicePaths: {}, return TSchemaPartitionResp: {}",
          devicePaths,
          result);

      return result;
    } else {
      return new TSchemaPartitionResp().setStatus(status);
    }
  }

  @Override
  public TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      GetNodePathsPartitionReq getNodePathsPartitionReq = new GetNodePathsPartitionReq();
      getNodePathsPartitionReq.setPartialPath(partialPath);
      if (null != level) {
        getNodePathsPartitionReq.setLevel(level);
      }
      SchemaNodeManagementResp resp =
          (SchemaNodeManagementResp)
              partitionManager.getNodePathsPartition(getNodePathsPartitionReq);
      TSchemaNodeManagementResp result =
          resp.convertToRpcSchemaNodeManagementPartitionResp(
              getLoadManager().genRealTimeRoutingPolicy());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "getNodePathsPartition receive devicePaths: {}, level: {}, return TSchemaNodeManagementResp: {}",
          partialPath,
          level,
          result);

      return result;
    } else {
      return new TSchemaNodeManagementResp().setStatus(status);
    }
  }

  @Override
  public TDataPartitionResp getDataPartition(GetDataPartitionReq getDataPartitionReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      DataPartitionResp resp =
          (DataPartitionResp) partitionManager.getDataPartition(getDataPartitionReq);

      TDataPartitionResp result =
          resp.convertToTDataPartitionResp(getLoadManager().genRealTimeRoutingPolicy());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "GetDataPartition interface receive PartitionSlotsMap: {}, return TDataPartitionResp: {}",
          getDataPartitionReq.getPartitionSlotsMap(),
          result);

      return result;
    } else {
      return new TDataPartitionResp().setStatus(status);
    }
  }

  @Override
  public TDataPartitionResp getOrCreateDataPartition(
      GetOrCreateDataPartitionReq getOrCreateDataPartitionReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      DataPartitionResp resp =
          (DataPartitionResp)
              partitionManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);

      TDataPartitionResp result =
          resp.convertToTDataPartitionResp(getLoadManager().genRealTimeRoutingPolicy());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "GetOrCreateDataPartition success. receive PartitionSlotsMap: {}, return TDataPartitionResp: {}",
          getOrCreateDataPartitionReq.getPartitionSlotsMap(),
          result);

      return result;
    } else {
      return new TDataPartitionResp().setStatus(status);
    }
  }

  private TSStatus confirmLeader() {
    if (getConsensusManager().isLeader()) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return new TSStatus(TSStatusCode.NEED_REDIRECTION.getStatusCode())
          .setMessage(
              "The current ConfigNode is not leader. And ConfigNodeGroup is in leader election. Please redirect with a random ConfigNode.");
    }
  }

  @Override
  public NodeManager getNodeManager() {
    return nodeManager;
  }

  @Override
  public ClusterSchemaManager getClusterSchemaManager() {
    return clusterSchemaManager;
  }

  @Override
  public ConsensusManager getConsensusManager() {
    return consensusManager;
  }

  @Override
  public PartitionManager getPartitionManager() {
    return partitionManager;
  }

  @Override
  public LoadManager getLoadManager() {
    return loadManager;
  }

  @Override
  public TSStatus operatePermission(AuthorReq authorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.operatePermission(authorReq);
    } else {
      return status;
    }
  }

  @Override
  public DataSet queryPermission(AuthorReq authorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.queryPermission(authorReq);
    } else {
      PermissionInfoResp dataSet = new PermissionInfoResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TPermissionInfoResp login(String username, String password) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.login(username, password);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(
      String username, List<String> paths, int permission) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.checkUserPrivileges(username, paths, permission);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    // Check global configuration
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TConfigNodeRegisterResp errorResp = new TConfigNodeRegisterResp();
    errorResp.setStatus(new TSStatus(TSStatusCode.ERROR_GLOBAL_CONFIG.getStatusCode()));
    if (!req.getDataRegionConsensusProtocolClass()
        .equals(conf.getDataRegionConsensusProtocolClass())) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the data_region_consensus_protocol_class "
                  + "are consistent.");
      return errorResp;
    }
    if (!req.getSchemaRegionConsensusProtocolClass()
        .equals(conf.getSchemaRegionConsensusProtocolClass())) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the schema_region_consensus_protocol_class "
                  + "are consistent.");
      return errorResp;
    }
    if (req.getSeriesPartitionSlotNum() != conf.getSeriesPartitionSlotNum()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the series_partition_slot_num are consistent.");
      return errorResp;
    }
    if (!req.getSeriesPartitionExecutorClass().equals(conf.getSeriesPartitionExecutorClass())) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the series_partition_executor_class are consistent.");
      return errorResp;
    }
    if (req.getDefaultTTL() != CommonDescriptor.getInstance().getConfig().getDefaultTTL()) {
      errorResp
          .getStatus()
          .setMessage("Reject register, please ensure that the default_ttl are consistent.");
      return errorResp;
    }
    if (req.getTimePartitionInterval() != conf.getTimePartitionInterval()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the time_partition_interval are consistent.");
      return errorResp;
    }
    if (req.getSchemaReplicationFactor() != conf.getSchemaReplicationFactor()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the schema_replication_factor are consistent.");
      return errorResp;
    }
    if (req.getDataReplicationFactor() != conf.getDataReplicationFactor()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the data_replication_factor are consistent.");
      return errorResp;
    }

    return nodeManager.registerConfigNode(req);
  }

  @Override
  public TSStatus applyConfigNode(ApplyConfigNodeReq applyConfigNodeReq) {
    return nodeManager.applyConfigNode(applyConfigNodeReq);
  }

  @Override
  public TSStatus createFunction(String udfName, String className, List<String> uris) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.createFunction(udfName, className, uris)
        : status;
  }

  @Override
  public TSStatus dropFunction(String udfName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.dropFunction(udfName)
        : status;
  }

  @Override
  public UDFManager getUDFManager() {
    return udfManager;
  }

  @Override
  public DataSet showRegion(GetRegionLocationsReq getRegionsinfoReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getRetionLocations(getRegionsinfoReq);
    } else {
      RegionLocationsResp regionResp = new RegionLocationsResp();
      regionResp.setStatus(status);
      return regionResp;
    }
  }

  public ProcedureManager getProcedureManager() {
    return procedureManager;
  }

  /**
   * @param storageGroups the storage groups to check
   * @return List of PartialPath the storage groups that not exist
   */
  public List<PartialPath> checkStorageGroupExist(List<PartialPath> storageGroups) {
    List<PartialPath> noExistSg = new ArrayList<>();
    if (storageGroups == null) {
      return noExistSg;
    }
    for (PartialPath storageGroup : storageGroups) {
      if (!clusterSchemaManager.getStorageGroupNames().contains(storageGroup.toString())) {
        noExistSg.add(storageGroup);
      }
    }
    return noExistSg;
  }

  @Override
  public void addMetrics() {
    partitionManager.addMetrics();
    nodeManager.addMetrics();
  }
}
