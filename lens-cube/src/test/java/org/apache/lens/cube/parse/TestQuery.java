package org.apache.lens.cube.parse;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;


import lombok.extern.slf4j.Slf4j;
import lombok.Getter;
import lombok.Setter;
import org.testng.Assert;

@Slf4j
public class TestQuery {

  private String actualQuery;
  @Getter
  @Setter
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

  /**
   * 1. Convert the characters of query into lower case characters
   * 2. remove all non characters
   * 3. replace innerjoin with join
   * 4. remove joinQueryPart from query
   *
   * @param query
   * @return
   */
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
    for ( JoinType joinType : JoinType.values()) {
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

/*
  private void compareJoinStrings(String actualJoinString, String expectedJoinString) {
    List<String> actualQueryParts =
            Lists.newArrayList(Splitter.on("join").trimResults().omitEmptyStrings().split(actualJoinString));
    List<String> expectedJoinList =
            Lists.newArrayList(Splitter.on("join").trimResults().omitEmptyStrings().split(expectedJoinString));
    Assert.assertEquals(actualQueryParts.size(), expectedJoinList.size());
    for (String joinStr : actualQueryParts) {
      Assert.assertTrue(expectedJoinList.contains(joinStr));
    }
  }
*/
/*
  void compareJoinQueries(String actual, String expected) {
    String expectedJoinString = extractJoinStringFromQuery(expected);
    compareJoinStrings(actualJoinString, expectedJoinString);
    String actualTrimmed =
            actual.toLowerCase().replaceAll("\\W", "").replaceAll("inner", "").replace(actualJoinString, "");
    String expectedTrimmed =
            expected.toLowerCase().replaceAll("\\W", "").replaceAll("inner", "").replace(expectedJoinString, "");
    if (!expectedTrimmed.equalsIgnoreCase(actualTrimmed)) {
      String method = null;
      for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
        if (trace.getMethodName().startsWith("test")) {
          method = trace.getMethodName() + ":" + trace.getLineNumber();
        }
      }

      System.err.println("__FAILED__ " + method + "\n\tExpected: " + expected + "\n\t---------\n\tActual: " + actual);
    }
    log.info("expectedTrimmed " + expectedTrimmed);
    log.info("actualTrimmed " + actualTrimmed);
    assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }
*/

  public boolean equals(TestQuery other) {
    if (this.actualQuery == null && other.actualQuery == null) {
      return true;
    } else if (this.actualQuery == null) {
      fail();
    } else if (other.actualQuery == null) {
      fail("Rewritten query is null");
    }
    assertEquals(trimmedQueryWitoutJoinString, other.trimmedQueryWitoutJoinString);
    assertEquals(this.joinTypeStrings, other.joinTypeStrings);
    return true;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Actual Query: " + actualQuery).append("\n");
    sb.append("JoinQueryString: " + joinTypeStrings);
    return sb.toString();
  }
}
