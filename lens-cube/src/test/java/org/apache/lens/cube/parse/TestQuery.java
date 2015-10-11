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

import lombok.Getter;
import lombok.Setter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestQuery {

  private String actualQuery;
  private String joinQueryPart = null;

  private String trimmedQuery = null;

  private Map<JoinType, Set<String>> joinTypeStrings = Maps.newTreeMap();

  private String preJoinQueryPart = null;

  private String postJoinQueryPart = null;

  public enum JoinType {
    INNERJOIN,
    LEFTOUTERJOIN,
    RIGHTOUTERJOIN,
    FULLOUTERJOIN,
    UNIQUE,
    LEFTSEMIJOIN,
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

  public TestQuery(String query) {
    this.actualQuery = query;
    this.trimmedQuery = getTrimmedQuery(query);
    this.joinQueryPart = extractJoinStringFromQuery(trimmedQuery);
    /**
     * Get the join query part, pre-join query and post-join query part from the trimmed query.
     *
     */
    if (trimmedQuery.indexOf(joinQueryPart) != -1) {
      this.preJoinQueryPart = trimmedQuery.substring(0, trimmedQuery.indexOf(joinQueryPart));
      if (getMinIndexOfClause() != -1) {
        this.postJoinQueryPart = trimmedQuery.substring(getMinIndexOfClause(), trimmedQuery.length() - 1);
      }
      prepareJoinStrings(trimmedQuery);
    }
  }

  private String getTrimmedQuery(String query) {
    return query.toUpperCase().replaceAll("\\W", "");
  }

  private void prepareJoinStrings(String query) {
    int index = 0;
    while (true) {
      JoinDetails joinDetails = getNextJoinTypeDetails(query);
      int nextJoinIndex = joinDetails.getIndex();
      if (joinDetails.getJoinType() == null) {
        log.info("Parsing joinQuery completed");
        return;
      }
      Set<String> joinStrings = joinTypeStrings.get(joinDetails.getJoinType());
      if (joinStrings == null) {
        joinStrings = Sets.newTreeSet();
        joinTypeStrings.put(joinDetails.getJoinType(), joinStrings);
      }
      joinStrings.add(joinDetails.getJoinString());
      index = nextJoinIndex;
      query = query.substring(nextJoinIndex+joinDetails.getJoinType().name().length());
    }
  }

  private class JoinDetails {
    @Setter @Getter private JoinType joinType;
    @Setter @Getter private int index;
    @Setter @Getter private String joinString;
  }

  /**
   * Get the next join query details from a given query
   */
  private JoinDetails getNextJoinTypeDetails(String query) {
    int nextJoinIndex = Integer.MAX_VALUE;
    JoinType nextJoinTypePart = null;
    for (JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(query, joinType.name(), 1);
      if (joinIndex < nextJoinIndex && joinIndex > 0) {
        nextJoinIndex = joinIndex;
        nextJoinTypePart = joinType;
      }
    }
    JoinDetails joinDetails = new JoinDetails();
    joinDetails.setIndex(nextJoinIndex);
    if (nextJoinIndex != Integer.MAX_VALUE) {
      joinDetails.setJoinString(
          getJoinString(query.substring(nextJoinIndex + nextJoinTypePart.name().length())));
    }
    joinDetails.setJoinType(nextJoinTypePart);
    return joinDetails;
  }

  private String getJoinString(String joinQueryStr) {
    int nextJoinIndex = Integer.MAX_VALUE;
    for (JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(joinQueryStr, joinType.name());
      if (joinIndex < nextJoinIndex && joinIndex > 0) {
        nextJoinIndex = joinIndex;
      }
    }
    if (nextJoinIndex == Integer.MAX_VALUE) {
      int minClauseIndex = getMinIndexOfClause(joinQueryStr);
      // return join query completely if there is no Clause in the query
      return minClauseIndex == -1 ? joinQueryStr : joinQueryStr.substring(0, minClauseIndex);
    }
    return joinQueryStr.substring(0, nextJoinIndex);
  }

  private int getMinIndexOfClause() {
    return getMinIndexOfClause(trimmedQuery);
  }

  private int getMinIndexOfClause(String query) {
    int minClauseIndex = Integer.MAX_VALUE;
    for (Clause clause : Clause.values()) {
      int clauseIndex = StringUtils.indexOf(query, clause.name());
      if (clauseIndex == -1) {
        continue;
      }
      minClauseIndex = clauseIndex < minClauseIndex ? clauseIndex : minClauseIndex;
    }
    return minClauseIndex == Integer.MAX_VALUE ? -1 : minClauseIndex;
  }

  private int getMinIndexOfJoinType() {
    int minJoinTypeIndex = Integer.MAX_VALUE;
    for (JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(trimmedQuery, joinType.name());
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
    boolean isEquals = Objects.equal(this.joinTypeStrings, expected.joinTypeStrings)
        && Objects.equal(this.preJoinQueryPart, expected.preJoinQueryPart)
        && Objects.equal(this.postJoinQueryPart, expected.postJoinQueryPart);
    if (!isEquals) {
      System.err.println("__FAILED__ " + "\n\tExpected: " + expected.toString()
          + "\n\t---------\n\tActual: " + this.toString());
    } else {
      System.err.println("SUCCEEDED " + "\n\tExpected: " + expected.toString()
          + "\n\t---------\n\tActual: " + this.toString());
    }
    return isEquals;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(actualQuery, joinQueryPart, trimmedQuery, joinTypeStrings);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Actual Query: " + actualQuery).append("\n");
    sb.append("JoinQueryString: " + joinTypeStrings);
    return sb.toString();
  }
}
