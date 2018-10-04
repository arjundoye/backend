package org.col.es.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BoolQuery extends AbstractQuery {

  private static class Clause extends QueryElement {
    private List<Query> must;
    @JsonProperty("must_not")
    private List<Query> mustNot;
    private List<Query> should;
  }

  private final Clause bool;

  public BoolQuery() {
    this.bool = new Clause();
  }

  public void must(Query query) {
    if (bool.must == null) {
      bool.must = new ArrayList<>();
    }
    bool.must.add(query);
  }

  public void mustNot(Query query) {
    if (bool.mustNot == null) {
      bool.mustNot = new ArrayList<>();
    }
    bool.mustNot.add(query);
  }

  public void should(Query query) {
    if (bool.should == null) {
      bool.should = new ArrayList<>();
    }
    bool.should.add(query);
  }

}
