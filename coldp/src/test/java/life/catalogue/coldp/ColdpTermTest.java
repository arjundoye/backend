package life.catalogue.coldp;

import org.junit.Test;

import static org.junit.Assert.*;
import static life.catalogue.coldp.ColdpTerm.*;

public class ColdpTermTest {
  @Test
  public void isClass() {
    for (ColdpTerm t : ColdpTerm.values()) {
      if (t.isClass()) {
        assertTrue(ColdpTerm.RESOURCES.containsKey(t));
      } else {
        assertFalse(ColdpTerm.RESOURCES.containsKey(t));
      }
    }
  }

  @Test
  public void find() {
    assertEquals(Taxon, ColdpTerm.find("taxon ", true));
    assertEquals(VernacularName, ColdpTerm.find("Vernacular-name", true));
    assertEquals(Treatment, ColdpTerm.find("treatment ", true));
    assertEquals(document, ColdpTerm.find("doc_ument ", false));
  }

  @Test
  public void higherRanks() {
    for (ColdpTerm t : DENORMALIZED_RANKS) {
      assertFalse(t.isClass());
      assertTrue(RESOURCES.get(ColdpTerm.Taxon).contains(t));
    }
  }

  @Test
  public void testNameUsage(){
    for (ColdpTerm t : RESOURCES.get(Name)) {
      if (t == genus) continue;
      assertTrue(RESOURCES.get(NameUsage).contains(t));
    }
    for (ColdpTerm t : RESOURCES.get(Taxon)) {
      if (t == provisional || t == nameID) continue;
      assertTrue(RESOURCES.get(NameUsage).contains(t));
    }
    for (ColdpTerm t : RESOURCES.get(Synonym)) {
      if (t == nameID || t == taxonID) continue;
      assertTrue(RESOURCES.get(NameUsage).contains(t));
    }
    // each term exists in at least one resource
    for (ColdpTerm t : ColdpTerm.values()) {
      if (t.isClass()) {
        assertTrue(t + " is no resource", RESOURCES.containsKey(t));

      } else {
        boolean found = false;
        for (var res : RESOURCES.values()) {
          if (res.contains(t)) {
            found = true;
            break;
          }
        }
        assertTrue(t + " without resource", found);
      }
    }

  }
}