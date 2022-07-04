package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Map;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 */
public class ShowNodesInSchemaTemplateTask implements IConfigTask {

  private final ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement;

  public ShowNodesInSchemaTemplateTask(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement) {
    this.showNodesInSchemaTemplateStatement = showNodesInSchemaTemplateStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showNodesInSchemaTemplate(this.showNodesInSchemaTemplateStatement);
  }

  public static void buildTSBlock(Template template, SettableFuture<ConfigTaskResult> future) {
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showNodesInSchemaTemplate.getRespDataTypes());
    try {
      if (template != null) {
        // template.get
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          String keyName = entry.getKey();
          IMeasurementSchema measurementSchema = entry.getValue();
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(new Binary(keyName));
          builder.getColumnBuilder(1).writeBinary(new Binary(measurementSchema.getType().name()));
          builder
              .getColumnBuilder(2)
              .writeBinary(new Binary(measurementSchema.getEncodingType().name()));
          builder
              .getColumnBuilder(3)
              .writeBinary(new Binary(measurementSchema.getCompressor().name()));
          builder.declarePosition();
        }
      }
    } catch (Exception e) {

    }
    DatasetHeader datasetHeader = HeaderConstant.showNodesInSchemaTemplate;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
