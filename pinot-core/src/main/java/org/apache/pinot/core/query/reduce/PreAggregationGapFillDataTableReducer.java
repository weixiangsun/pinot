/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.reduce;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PreAggregationGapFillDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;

  PreAggregationGapFillDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
  }

  /**
   * Reduces data tables and sets aggregations results into
   * 1. ResultTable if _responseFormatSql is true
   * 2. AggregationResults by default
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      DataSchema resultTableSchema =
          new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema()).getResultDataSchema();
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    PreAggregationGapFillSelectionOperatorService preAggregationGapFillSelectionOperatorService
        = new PreAggregationGapFillSelectionOperatorService(_queryContext, dataSchema);
    List<Object[]> sortedRawRows = preAggregationGapFillSelectionOperatorService.reduceWithOrdering(dataTableMap.values());

    List<Object[]> gapfilledSortedRawRows = preAggregationGapFillSelectionOperatorService.gapFill(sortedRawRows);

    List<Object[]> aggregateResult = preAggregationGapFillSelectionOperatorService.aggregate(gapfilledSortedRawRows);

    String [] columnNames = new String[2];
    columnNames[0] = _queryContext.getSelectExpressions().get(0).getFunction().getArguments().get(0).toString();
    columnNames[1] = _queryContext.getSelectExpressions().get(1).getFunction().getFunctionName();
    FunctionContext secondFunctionContext = _queryContext.getSelectExpressions().get(1).getFunction();
    AggregationFunction secondAggregationFunction =
        AggregationFunctionFactory.getAggregationFunction(secondFunctionContext, _queryContext);
    ColumnDataType [] ColumnDataTypes = new ColumnDataType[2];
    ColumnDataTypes[0] = ColumnDataType.STRING;
    ColumnDataTypes[1] = secondAggregationFunction.getFinalResultColumnType();
    // Merge results from all data tables
    brokerResponseNative.setResultTable(new ResultTable(new DataSchema(columnNames, ColumnDataTypes), aggregateResult));
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  private DataSchema getPrePostAggregationDataSchema() {
    int numAggregationFunctions = _aggregationFunctions.length;
    String[] columnNames = new String[numAggregationFunctions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      columnNames[i] = aggregationFunction.getResultColumnName();
      columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }
}
