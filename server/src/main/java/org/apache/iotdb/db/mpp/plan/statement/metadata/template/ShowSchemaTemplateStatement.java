package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStatement;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 */
public class ShowSchemaTemplateStatement extends ShowStatement implements IConfigStatement {

  public ShowSchemaTemplateStatement() {
    super();
    statementType = StatementType.SHOW_SCHEMA_TEMPLATE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowSchemaTemplate(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }
}
