package org.apache.lens.cube.parse;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.Getter;
import lombok.Setter;
import org.testng.Assert;

import javax.jdo.annotations.Join;

@Slf4j
public class TestQuery {

  String actualQuery;
  @Getter
  @Setter
  String joinQueryPart = null;

  String trimmedQuery = null;

  Map<JoinType, List<String>> joinTypeStrings;

  private static Map<JoinType, String> joinTypeToJoinString = Maps.newHashMap();

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
    this.joinQueryPart = extractJoinStringFromQuery(query);
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

  void compareJoinQueries(String actual, String expected) {
    String actualJoinString = extractJoinStringFromQuery(actual);
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

  private int getNextJoinTypeIndex(String query, int index) {
    int nextJoinIndex = Integer.MAX_VALUE;
    for ( JoinType joinType : JoinType.values()) {
      int joinIndex = StringUtils.indexOf(query, joinTypeToJoinString.get(joinType));
      nextJoinIndex = joinIndex < nextJoinIndex ? joinIndex : nextJoinIndex;
    }
    return nextJoinIndex > index ? nextJoinIndex : index;
  }

  private int getMinIndexOfClause() {
    String query = actualQuery.toLowerCase().replaceAll("\\W", "");
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

  private String extractJoinStringFromQuery(String query) {
    String queryTrimmed = query.toLowerCase().replaceAll("\\W", "");
    int joinStartIndex = getMinIndexOfJoinType();
    int joinEndIndex = getMinIndexOfClause();
    return StringUtils.substring(queryTrimmed, joinStartIndex, joinEndIndex);
  }

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

  public void equals(TestQuery other) {
    if (this.trimmedQuery == null && other.trimmedQuery == null) {
      return;
    } /*else if (expected == null) {
      fail();
    } else if (actual == null) {
      fail("Rewritten query is null");
    }*/
    //compareJoinQueries(actual, expected);
  }
}
