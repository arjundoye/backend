package life.catalogue.matching;

import io.dropwizard.lifecycle.Managed;
import life.catalogue.api.model.IndexName;

import java.util.ArrayList;

public interface NameIndexStore extends Managed {
  
  /**
   * Counts all name usages. Potentially an expensive operation.
   */
  int count();
  
  ArrayList<IndexName> get(String key);
  
  boolean containsKey(String key);
  
  void put(String key, ArrayList<IndexName> group);
}