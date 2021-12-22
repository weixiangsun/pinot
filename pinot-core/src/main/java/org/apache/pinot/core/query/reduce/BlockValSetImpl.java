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

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class BlockValSetImpl implements BlockValSet {

  private final FieldSpec.DataType _dataType;
  private final List<Object[]> _rows;
  private final int _columnIndex;

  public BlockValSetImpl(DataSchema.ColumnDataType columnDataType, List<Object[]> rows, int columnIndex) {
    _dataType = columnDataType.toDataType();
    _rows = rows;
    _columnIndex = columnIndex;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    //TODO: need fix it.
    return true;
  }

  @org.jetbrains.annotations.Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    return new int[0];
  }

  @Override
  public int[] getIntValuesSV() {
    if (_dataType == FieldSpec.DataType.INT) {
      int [] result = new int[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = (Integer)_rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public long[] getLongValuesSV() {
    if (_dataType == FieldSpec.DataType.LONG) {
      long [] result = new long[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = (Long)_rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public float[] getFloatValuesSV() {
    if (_dataType == FieldSpec.DataType.FLOAT) {
      float [] result = new float[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = (Float) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public double[] getDoubleValuesSV() {
    if (_dataType == FieldSpec.DataType.DOUBLE) {
      double [] result = new double[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = (Double) _rows.get(i)[_columnIndex];
      }
      return result;
    } else if(_dataType == FieldSpec.DataType.INT) {
      double [] result = new double[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = ((Integer) _rows.get(i)[_columnIndex]).doubleValue();
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public String[] getStringValuesSV() {
    if (_dataType == FieldSpec.DataType.STRING) {
      String [] result = new String[_rows.size()];
      for(int i = 0; i < result.length; i ++) {
        result[i] = (String) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public byte[][] getBytesValuesSV() {
    return new byte[0][];
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    return new int[0][];
  }

  @Override
  public int[][] getIntValuesMV() {
    return new int[0][];
  }

  @Override
  public long[][] getLongValuesMV() {
    return new long[0][];
  }

  @Override
  public float[][] getFloatValuesMV() {
    return new float[0][];
  }

  @Override
  public double[][] getDoubleValuesMV() {
    return new double[0][];
  }

  @Override
  public String[][] getStringValuesMV() {
    return new String[0][];
  }

  @Override
  public int[] getNumMVEntries() {
    return new int[0];
  }
}
