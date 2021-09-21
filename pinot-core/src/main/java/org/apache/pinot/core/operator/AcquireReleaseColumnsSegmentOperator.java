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
package org.apache.pinot.core.operator;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * A common wrapper around the segment-level operator.
 * Provides an opportunity to acquire and release column buffers before reading data
 */
public class AcquireReleaseColumnsSegmentOperator extends BaseOperator {
  private static final String OPERATOR_NAME = "AcquireReleaseColumnsSegmentOperator";

  private final PlanNode _planNode;
  private final IndexSegment _indexSegment;
  private final FetchContext _fetchContext;
  private Operator _childOperator;

  public AcquireReleaseColumnsSegmentOperator(PlanNode planNode, IndexSegment indexSegment, FetchContext fetchContext) {
    _planNode = planNode;
    _indexSegment = indexSegment;
    _fetchContext = fetchContext;
  }

  /**
   * Makes a call to acquire column buffers from {@link IndexSegment} before getting nextBlock from childOperator,
   * and
   * a call to release the column buffers from {@link IndexSegment} after.
   */
  @Override
  protected Block getNextBlock() {
    _childOperator = _planNode.run();
    return _childOperator.nextBlock();
  }

  public void acquire() {
    _indexSegment.acquire(_fetchContext);
  }

  public void release() {
    _indexSegment.release(_fetchContext);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _childOperator == null ? new ExecutionStatistics(0, 0, 0, 0) : _childOperator.getExecutionStatistics();
  }
}
