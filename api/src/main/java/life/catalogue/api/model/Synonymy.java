package life.catalogue.api.model;

import java.util.*;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A taxonomic synonymy list, ordering names in homotypic groups.
 * All synonym names excluding misapplied names can be iterated over.
 */
public class Synonymy implements Iterable<Name> {
  private final List<Name> homotypic = new ArrayList<>();
  private final List<List<Name>> heterotypic = new ArrayList<>();
  private final List<NameUsageBase> misapplied = new ArrayList<>();
  
  @JsonIgnore
  public boolean isEmpty() {
    return homotypic.isEmpty() && heterotypic.isEmpty() && misapplied.isEmpty();
  }
  
  public List<Name> getHomotypic() {
    return homotypic;
  }

  public void addHomotypic(Name name) {
    this.homotypic.add(name);
  }

  public List<List<Name>> getHeterotypic() {
    return heterotypic;
  }
  
  public List<NameUsageBase> getMisapplied() {
    return misapplied;
  }
  
  public void addMisapplied(NameUsageBase misapplied) {
    this.misapplied.add(misapplied);
  }
  
  /**
   * Adds a new homotypic group of names to the heterotypic synonyms
   *
   * @param synonyms
   */
  public void addHeterotypicGroup(List<Name> synonyms) {
    this.heterotypic.add(synonyms);
  }
  
  public int size() {
    return homotypic.size()
        + misapplied.size()
        + heterotypic.stream()
        .mapToInt(List::size)
        .sum();
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Synonymy synonymy = (Synonymy) o;
    return Objects.equals(homotypic, synonymy.homotypic) &&
        Objects.equals(heterotypic, synonymy.heterotypic) &&
        Objects.equals(misapplied, synonymy.misapplied);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(homotypic, heterotypic, misapplied);
  }
  
  @Override
  public Iterator<Name> iterator() {
    return Stream.concat(
        homotypic.stream(),
        heterotypic.stream().flatMap(Collection::stream)
    ).iterator();
  }
}
