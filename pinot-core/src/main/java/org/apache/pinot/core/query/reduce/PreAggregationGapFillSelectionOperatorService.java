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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * The <code>SelectionOperatorService</code> class provides the services for selection queries with
 * <code>ORDER BY</code>.
 * <p>Expected behavior:
 * <ul>
 *   <li>
 *     Return selection results with the same order of columns as user passed in.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For 'SELECT *', return columns with alphabetically order.
 *     <ul>
 *       <li>Eg. SELECT * FROM table -> [valA, valB, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Order by does not change the order of columns in selection results.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@SuppressWarnings("rawtypes")
public class PreAggregationGapFillSelectionOperatorService {
  private final List<String> _columns;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Object[]> _rows;

  private final DateTimeGranularitySpec _dateTimeGranularity;
  private final DateTimeFormatSpec _dateTimeFormatter;
  private final long _startMs;
  private final long _endMs;
  private final long _timeBucketSize;
  private final QueryContext _queryContext;

  private final int _numOfGroupByKeys;
  private final List<Integer> _groupByKeyIndexes;
  private final boolean [] _isGroupBySelections;
  private final Set<Key> _groupByKeys;
  private final List<Key> _groupByKeyList;
  private final Map<Key, Integer> _groupByKeyMappings;
  private final Map<Key, Object[]> _previousByGroupKey;
  private Map<String, ExpressionContext> fillExpressions;
  private final FilterContext _gapFillFilterContext;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param queryContext Selection order-by query
   * @param dataSchema data schema.
   */
  public PreAggregationGapFillSelectionOperatorService(QueryContext queryContext, DataSchema dataSchema) {
    _columns = Arrays.asList(dataSchema.getColumnNames());
    _dataSchema = dataSchema;
    // Select rows from offset to offset + limit.
    _offset = queryContext.getOffset();
    _numRowsToKeep = _offset + queryContext.getLimit();
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator());

    _queryContext = queryContext;

    ExpressionContext gapFillSelection = queryContext.getSelectExpressions().get(0);
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    _gapFillFilterContext = GapfillUtils.getFilterContext(_queryContext);
    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    _dateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    String start = args.get(2).getLiteral();
    String end = args.get(3).getLiteral();
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _timeBucketSize = _dateTimeGranularity.granularityToMillis();

    _previousByGroupKey = new HashMap<>();
    ExpressionContext timeseriesOn = _queryContext.getSelectExpressions().get(0).getFunction().getArguments().get(5);
    _numOfGroupByKeys = timeseriesOn.getFunction().getArguments().size() - 1;
    _groupByKeyIndexes = new ArrayList<>();
    _isGroupBySelections = new boolean[dataSchema.getColumnDataTypes().length];
    _groupByKeys = new HashSet<>();
    _groupByKeyList = new ArrayList<>();
    _groupByKeyMappings = new HashMap<>();

    Map<String, Integer> indexes = new HashMap<>();
    for(int i = 0; i < _columns.size(); i ++) {
      indexes.put(_columns.get(i), i);
    }

    List<ExpressionContext> timeSeries = timeseriesOn.getFunction().getArguments();
    for(int i = 1; i < timeSeries.size(); i ++) {
      int index = indexes.get(timeSeries.get(i).getIdentifier());
      _isGroupBySelections[index] = true;
      _groupByKeyIndexes.add(index);
    }

    fillExpressions = new HashMap<>();

    for(int i = 6; i < gapFillSelection.getFunction().getArguments().size(); i ++) {
      ExpressionContext fillExpression = gapFillSelection.getFunction().getArguments().get(i);
      fillExpressions.put(fillExpression.getFunction().getArguments().get(0).getIdentifier(), fillExpression);
    }
  }

  private Key constructGroupKeys(Object[] row) {
    Object [] groupKeys = new Object[_numOfGroupByKeys];
    for (int i = 0; i < _numOfGroupByKeys; i++) {
      groupKeys[i] = row[_groupByKeyIndexes.get(i)];
    }
    return new Key(groupKeys);
  }

  private long truncate(long epoch) {
    int sz = _dateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  List<Object[]> gapFill(List<Object[]> sortedRows) {
    Iterator<Object[]> sortedIterator = sortedRows.iterator();
    ColumnDataType[] resultColumnDataTypes = _dataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;
    List<Object[]> gapfillResultRows = new ArrayList<>();

    FilterContext havingFilter = _queryContext.getHavingFilter();
    HavingFilterHandler havingFilterHandler = null;
    if (_gapFillFilterContext != null) {
      PostAggregationHandler postAggregationHandler =
          new PostAggregationHandler(_queryContext, _dataSchema);
      havingFilterHandler = new HavingFilterHandler(_gapFillFilterContext, postAggregationHandler);
    }
    Object[] row = null;
    for (long time = _startMs; time < _endMs; time += _timeBucketSize) {
      Set<Key> keys = new HashSet<>(_groupByKeys);
      if (row == null && sortedIterator.hasNext()) {
        row = sortedIterator.next();
      }

      while (row != null) {
        Object[] resultRow = row;
        for (int i = 0; i < resultColumnDataTypes.length; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }

        long timeCol = _dateTimeFormatter.fromFormatToMillis(String.valueOf(resultRow[1]));
        if (timeCol > time) {
          break;
        }
        if (timeCol == time) {
          if (havingFilterHandler == null || havingFilterHandler.isMatch(row)) {
            gapfillResultRows.add(resultRow);
          }
          Key key = constructGroupKeys(resultRow);
          keys.remove(key);
          _previousByGroupKey.put(key, resultRow);
        }
        if (sortedIterator.hasNext()) {
          row = sortedIterator.next();
        } else {
          row = null;
        }
      }

      for (Key key : keys) {
        Object[] gapfillRow = new Object[numResultColumns];
        int keyIndex = 0;
        gapfillRow[0] = time;
        if (resultColumnDataTypes[1] == ColumnDataType.LONG) {
          gapfillRow[1] = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(time));
        } else {
          gapfillRow[1] = _dateTimeFormatter.fromMillisToFormat(time);
        }
        for (int i = 2; i < _isGroupBySelections.length; i++) {
          if (_isGroupBySelections[i]) {
              gapfillRow[i] = key.getValues()[keyIndex++];
          } else {
            gapfillRow[i] = getFillValue(i, _dataSchema.getColumnName(i), key, resultColumnDataTypes[i]);
          }
        }

        if (havingFilterHandler == null || havingFilterHandler.isMatch(gapfillRow)) {
          gapfillResultRows.add(gapfillRow);
        }
      }
    }
    return gapfillResultRows;
  }

  List<Object[]> aggregate(List<Object[]> gapfillRows) {
    List<Object[]> result = new ArrayList<>();
    int index = 0;
    for(long time = _startMs; time < _endMs; time += _timeBucketSize) {
      Object[] resultRow = new Object[_queryContext.getSelectExpressions().size()];
      String formattedTime = _dateTimeFormatter.fromMillisToFormat(time);
      resultRow[0] = formattedTime;
      List<Object[]> bucketedRows = new ArrayList<>();
      while(index < gapfillRows.size()) {
        if(formattedTime.equals(gapfillRows.get(index)[1])) {
          bucketedRows.add(gapfillRows.get(index));
          index ++;
        } else {
          break;
        }
      }
      Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
      blockValSetMap.put(ExpressionContext.forIdentifier(_dataSchema.getColumnName(0)), new BlockValSetImpl(_dataSchema.getColumnDataType(0), bucketedRows, 0));
      for(int i = 2; i < _dataSchema.getColumnNames().length; i ++) {
        blockValSetMap.put(ExpressionContext.forIdentifier(_dataSchema.getColumnName(i)), new BlockValSetImpl(_dataSchema.getColumnDataType(i), bucketedRows, i));
      }

      for(int i = 1; i < _queryContext.getSelectExpressions().size(); i ++) {
        ExpressionContext aggregationExpressionContext = _queryContext.getSelectExpressions().get(i);
        FunctionContext secondFunctionContext = aggregationExpressionContext.getFunction();
        FunctionContext firstFunctionContext = secondFunctionContext.getArguments().get(0).getFunction();
        AggregationFunction secondAggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(secondFunctionContext, _queryContext);
        AggregationFunction firstAggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(firstFunctionContext, _queryContext);
        GroupByResultHolder firstGroupByResultHolder = firstAggregationFunction.createGroupByResultHolder(_groupByKeys.size(), _groupByKeys.size());
        GroupByResultHolder secondGroupByResultHolder = secondAggregationFunction.createGroupByResultHolder(1,1);
        int [] groupKeyArray = new int[bucketedRows.size()];
        for(int bucketIndex = 0; bucketIndex < groupKeyArray.length; bucketIndex ++) {
          groupKeyArray[bucketIndex] = _groupByKeyMappings.get(constructGroupKeys(bucketedRows.get(bucketIndex)));
        }
        firstAggregationFunction.aggregateGroupBySV(bucketedRows.size(), groupKeyArray, firstGroupByResultHolder, blockValSetMap);
        List<Object[]> aggregatedRows = new ArrayList<>();
        for(int groupIndex = 0; groupIndex < _groupByKeys.size(); groupIndex ++) {
          Object[] aggregatedRow = new Object[3];
          aggregatedRow[0] = formattedTime;
          aggregatedRow[1] = _groupByKeyList.get(groupKeyArray[groupIndex]);
          aggregatedRow[2] = firstAggregationFunction.extractGroupByResult(firstGroupByResultHolder, groupKeyArray[groupIndex]);
          aggregatedRow[2] = firstAggregationFunction.extractFinalResult(aggregatedRow[2]);
          aggregatedRows.add(aggregatedRow);
        }
        Map<ExpressionContext, BlockValSet> aggregatedBlockValSetMap = new HashMap<>();
        aggregatedBlockValSetMap.put(secondFunctionContext.getArguments().get(0), new BlockValSetImpl(firstAggregationFunction.getFinalResultColumnType(), aggregatedRows, 2));
        int [] secondGroupKeyArray = new int[aggregatedRows.size()];
        secondAggregationFunction.aggregateGroupBySV(aggregatedRows.size(), secondGroupKeyArray, secondGroupByResultHolder, aggregatedBlockValSetMap);
        resultRow[1] = secondAggregationFunction.extractGroupByResult(secondGroupByResultHolder, 0);
        resultRow[1] = secondAggregationFunction.extractFinalResult(resultRow[1]);
      }
      result.add(resultRow);
    }
    return result;
  }

  Object getFillValue(int columnIndex, String columnName, Object key, ColumnDataType dataType) {
    ExpressionContext expressionContext = fillExpressions.get(columnName);
    if (expressionContext != null && expressionContext.getFunction() != null && GapfillUtils.isFill(expressionContext)) {
      List<ExpressionContext> args = expressionContext.getFunction().getArguments();
      if (args.get(1).getLiteral() == null) {
        throw new UnsupportedOperationException("Wrong Sql.");
      }
      GapfillUtils.FillType fillType = GapfillUtils.FillType.valueOf(args.get(1).getLiteral());
      if (fillType == GapfillUtils.FillType.FILL_DEFAULT_VALUE) {
        // TODO: may fill the default value from sql in the future.
        return GapfillUtils.getDefaultValue(dataType);
      } else if (fillType == GapfillUtils.FillType.FILL_PREVIOUS_VALUE) {
        Object[] row = _previousByGroupKey.get(key);
        if (row != null) {
          return row[columnIndex];
        } else {
          return GapfillUtils.getDefaultValue(dataType);
        }
      } else {
        throw new UnsupportedOperationException("unsupported fill type.");
      }
    } else {
      return GapfillUtils.getDefaultValue(dataType);
    }
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  private Comparator<Object[]> getTypeCompatibleComparator() {
    ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();

    return (o1, o2) -> {
      Object v1 = o1[0];
      Object v2 = o2[0];
      int result;
      if (columnDataTypes[0].isNumber()) {
        result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
      } else {
        //noinspection unchecked
        result = ((Comparable) v1).compareTo(v2);
      }
      return result;
    };
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public PriorityQueue<Object[]> getRows() {
    return _rows;
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   */
  public List<Object[]> reduceWithOrdering(Collection<DataTable> dataTables) {
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        _rows.add(row);
      }
    }
    LinkedList<Object[]> sortedRows = new LinkedList<>();
    while(!_rows.isEmpty()) {
      Object[] row = _rows.poll();
      sortedRows.add(row);
      Key k = constructGroupKeys(row);
      if(!_groupByKeys.contains(k)) {
        _groupByKeys.add(k);
        _groupByKeyMappings.put(k, _groupByKeyList.size());
        _groupByKeyList.add(k);
      }
    }
    return sortedRows;
  }
}
