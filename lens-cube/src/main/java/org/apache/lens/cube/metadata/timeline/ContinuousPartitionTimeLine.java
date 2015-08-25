package org.apache.lens.cube.metadata.timeline;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import lombok.NonNull;

public class ContinuousPartitionTimeLine extends PartitionTimeline {

  private TimePartition first;
  private TimePartition latest;

  public ContinuousPartitionTimeLine(String storageTableName, UpdatePeriod updatePeriod, String partCol) {
    super(storageTableName, updatePeriod, partCol);
  }

  @Override
  public boolean add(@NonNull TimePartition partition) throws LensException {
    return false;
  }

  @Override
  public boolean drop(@NonNull TimePartition toDrop) throws LensException {
    return false;
  }

  @Override
  public TimePartition latest() {
    return ;
  }

  @Override
  public Map<String, String> toProperties() {
    return null;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    first = null;
    latest = null;
    String firstStr = properties.get("first");
    String latestStr = properties.get("latest");
    if (!Strings.isNullOrEmpty(firstStr)) {
      first = TimePartition.of(getUpdatePeriod(), firstStr);
    }
    if (!Strings.isNullOrEmpty(latestStr)) {
      latest = TimePartition.of(getUpdatePeriod(), latestStr);
    }
    return isConsistent();
  }

  @Override
  public boolean isEmpty() {
    return latest != null;
  }

  @Override
  public boolean isConsistent() {
    return !latest.before(first);
  }

  @Override
  public boolean exists(TimePartition partition) {
    throw new UnsupportedOperationException("No partitions exists for continuous update period ");
  }

  @Override
  public Iterator<TimePartition> iterator() {
    throw new UnsupportedOperationException("No supported");
  }
}
