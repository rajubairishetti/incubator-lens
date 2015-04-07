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
package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Optional;

import lombok.Getter;

public class BaseDimAttribute extends CubeDimAttribute {
  @Getter private final String type;
  @Getter private Optional<Long> numOfDistinctValues;

  public BaseDimAttribute(FieldSchema column) {
    this(column, null, null, null, null);
  }

  public BaseDimAttribute(FieldSchema column, String displayString, Date startTime, Date endTime, Double cost) {
    super(column.getName(), column.getComment(), displayString, startTime, endTime, cost);
    this.type = column.getType();
    assert (type != null);
  }

  public BaseDimAttribute(FieldSchema column, String displayString, Date startTime, Date endTime, Double cost,
      Optional<Long> numOfDistinctValues) {
    super(column.getName(), column.getComment(), displayString, startTime, endTime, cost);
    this.type = column.getType();
    assert (type != null);

    System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA value: " + numOfDistinctValues);
    if (numOfDistinctValues.isPresent()) {
      this.numOfDistinctValues = numOfDistinctValues;
      System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCC   " + this.numOfDistinctValues);
      assert(this.numOfDistinctValues.get() > 0);
    }
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimTypePropertyKey(getName()), type);
    if (isSetNumOfDistinctValues()) {
      System.out.println("BCCCCCCCCCCBBBBBBBBBBBBBBBBBBBBBBB  putttitttnggggg "
          + MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(getName())
          + "   valuuuuuue " +String.valueOf(numOfDistinctValues.get()));
      props.put(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(getName()),
          String.valueOf(numOfDistinctValues.get()));
    }
  }

  private boolean isSetNumOfDistinctValues() {
    System.out.println("BBBBBBBBBBBBBBBBBB   " + numOfDistinctValues);
    //return numOfDistinctValues.orNull() != null) {
    return numOfDistinctValues != null && numOfDistinctValues.isPresent();
   // return !Optional.fromNullable(numOfDistinctValues).equals(Optional.absent());
   // return numOfDistinctValues.orNull() != null && numOfDistinctValues.isPresent();
    /*return (numOfDistinctValues != null
        && !numOfDistinctValues.equals(MetastoreConstants.DEFAULT_NUM_OF_DISTINCT_VALUES));*/
  }

  /**
   * This is used only for serializing
   *
   * @param name
   * @param props
   */
  public BaseDimAttribute(String name, Map<String, String> props) {
    super(name, props);
    this.type = getDimType(name, props);
    Optional<Long> numDistinctValues = getDimNumOfDistinctValues(name, props);
    if (numDistinctValues.isPresent()) {
      this.numOfDistinctValues = numDistinctValues;
    }
  }

  public static String getDimType(String name, Map<String, String> props) {
    return props.get(MetastoreUtil.getDimTypePropertyKey(name));
  }

  public static Optional<Long> getDimNumOfDistinctValues(String name, Map<String, String> props) {
    if (props.containsKey(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(name))) {
      return Optional.of(Long.parseLong((props.get(MetastoreUtil.getDimNumOfDistinctValuesPropertyKey(name)))));
    }
    return Optional.absent();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 : getType().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    BaseDimAttribute other = (BaseDimAttribute) obj;
    if (this.getType() == null) {
      if (other.getType() != null) {
        return false;
      }
    } else if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString() + ":" + getType() + ":" + getNumOfDistinctValues();
    return str;
  }
}
