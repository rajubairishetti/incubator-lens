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

package org.apache.lens.cube.parse;

import static org.testng.Assert.fail;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestQuery {

  private String actualQuery;
  private String joinQueryPart = null;

  private String trimmedQuery = null;

  private String trimmedQueryWitoutJoinString = null;

  private Map<JoinType, Set<String>> joinTypeStrings = Maps.newTreeMap();

  private static Map<JoinType, String> joinTypeToJoinString = Maps.newTreeMap();

  public enum JoinType {
    INNER,
    LEFTOUTER,
    RIGHTOUTER,
    FULLOUTER,
    UNIQUE,
    LEFTSEMI,
    JOIN;

    private JoinType() {
    }
  }

  public enum Clause {
    WHERE,
    GROUPBY,
    HAVING,
    ORDEREDBY;

    private Clause() {

    }
  }

  static {
    for (JoinType joinType : JoinType.values()) {
      joinTypeToJoinString.put(joinType, joinType.name().toLowerCase());
    }
  }

  public TestQuery(String query) {
    this.actualQuery = query;
    this.trimmedQuery = getTrimmedQuery(query);
    this.joinQueryPart = extractJoinStringFromQuery(trimmedQuery);
    // remove join string part from the query
    this.trimmedQueryWitoutJoinString = trimmedQuery.replace(joinQueryPart, "");
    prepareJoinStrings(trimmedQuery);
  }

  private String getTrimmedQuery(String query) {
    return query.toLowerCase().replaceAll("\\W", "");
  }

  private void prepareJoinStrings(String query) {
    int index = 0;
    for (JoinType joinType : JoinType.values()) {
      int nextJoinIndex = getNextJoinTypeIndex(query, index);
      if (nextJoinIndex == Integer.MAX_VALUE) {
        log.info("Parsing joinQuery completed");
        return;
      }
      Set<String> joinStrings = joinTypeStrings.get(joinType);
      if (joinStrings == null) {
        joinStrings = Sets.newTreeSet();
      }
      joinStrings.add(query.substring(index, nextJoinIndex));
    }
  }

  private int getNextJoinTypeIndex(String query, int index) {
    int nextJoinIndex = Integer.MAX_VALUE;
    for (JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(query, joinTypeToJoinString.get(joinType));
      nextJoinIndex = joinIndex < nextJoinIndex ? joinIndex : nextJoinIndex;
    }
    return nextJoinIndex > index ? nextJoinIndex : index;
  }

  private int getMinIndexOfClause() {
    String query = trimmedQuery;
    int minClauseIndex = Integer.MAX_VALUE;
    for (Clause clause : Clause.values()) {
      int clauseIndex = StringUtils.indexOf(query, clause.toString().toLowerCase());
      if (clauseIndex == -1) {
        continue;
      }
      minClauseIndex = clauseIndex < minClauseIndex ? clauseIndex : minClauseIndex;
    }
    return minClauseIndex == Integer.MAX_VALUE ? -1 : minClauseIndex;
  }

  private int getMinIndexOfJoinType() {
    String query = actualQuery.toLowerCase().replaceAll("\\W", "");
    int minJoinTypeIndex = Integer.MAX_VALUE;
    for (JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(query, joinTypeToJoinString.get(joinType));
      if (joinIndex == -1) {
        continue;
      }
      minJoinTypeIndex = joinIndex < minJoinTypeIndex ? joinIndex : minJoinTypeIndex;
    }
    return minJoinTypeIndex == Integer.MAX_VALUE ? -1 : minJoinTypeIndex;
  }

  private String extractJoinStringFromQuery(String queryTrimmed) {
    int joinStartIndex = getMinIndexOfJoinType();
    int joinEndIndex = getMinIndexOfClause();
    if (joinStartIndex == -1 && joinEndIndex == -1) {
      return queryTrimmed;
    }
    return StringUtils.substring(queryTrimmed, joinStartIndex, joinEndIndex);
  }

  public boolean equals(TestQuery expected) {
    if (this == expected) {
      return true;
    }
    if (this.actualQuery == null && expected.actualQuery == null) {
      return true;
    } else if (this.actualQuery == null) {
      fail();
    } else if (expected.actualQuery == null) {
      fail("Rewritten query is null");
    }
    return Objects.equal(this.trimmedQueryWitoutJoinString, expected.trimmedQueryWitoutJoinString)
            && Objects.equal(this.joinTypeStrings, expected.joinTypeStrings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(actualQuery, joinQueryPart, trimmedQuery, trimmedQueryWitoutJoinString, joinTypeStrings);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Actual Query: " + actualQuery).append("\n");
    sb.append("JoinQueryString: " + joinTypeStrings);
    return sb.toString();
  }
}
