package life.catalogue.db.mapper.legacy.mapper;

import life.catalogue.db.LookupTables;
import life.catalogue.db.TestDataRule;
import life.catalogue.db.mapper.MapperTestBase;
import life.catalogue.db.mapper.legacy.LNameMapper;
import life.catalogue.db.mapper.legacy.model.LHigherName;
import life.catalogue.db.mapper.legacy.model.LName;
import life.catalogue.db.mapper.legacy.model.LSpeciesName;
import org.gbif.nameparser.api.Rank;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class LNameMapperTest extends MapperTestBase<LNameMapper> {

  int datasetKey = TestDataRule.TestData.FISH.key;

  public LNameMapperTest() {
    super(LNameMapper.class, TestDataRule.fish());
  }

  @Before
  public void init () throws IOException, SQLException {
    LookupTables.recreateTables(session().getConnection());
  }

  @Test
  public void get() {
    LSpeciesName n = (LSpeciesName) mapper().get(datasetKey, "u100");
    assertEquals("u100", n.getId());
    assertEquals(Rank.SPECIES, n.getRank());

    LHigherName hn = (LHigherName) mapper().get(datasetKey, "u10");
    assertEquals("u10", hn.getId());
    assertEquals(Rank.GENUS, hn.getRank());

    n = (LSpeciesName) mapper().getFull(datasetKey, "u100");
    assertEquals("u100", n.getId());
    assertEquals(Rank.SPECIES, n.getRank());
    assertEquals("Chromis (Chromis) agilis", n.getName());
    assertEquals("<i>Chromis (Chromis) agilis</i> Smith, 1960", n.getNameHtml());
    assertEquals("Chromis", n.getGenus());
    assertEquals("Chromis", n.getSubgenus());
    assertEquals("agilis", n.getSpecies());
    assertNull(n.getInfraspecies());
    assertNull(n.getInfraspeciesMarker());
    assertEquals("Smith, 1960", n.getAuthor());
    assertEquals("http://fish.org/100", n.getOnlineResource());

    assertEquals("FishBase", n.getSourceDatabase());
    assertEquals("http://fishbase.de", n.getSourceDatabaseUrl());
    assertNull(n.getRecordScrutinyDate());

    //assertNull(n.getBibliographicCitation());
    assertEquals(1, n.getChildTaxa().size());
    n.getChildTaxa().forEach(this::assertPresent);

    assertEquals(5, n.getClassification().size());
    n.getClassification().forEach(this::assertPresent);
    //assertEquals(Rank.SPECIES, n.getCommonNames());
    //assertEquals(Rank.SPECIES, n.getDistribution());


    n = (LSpeciesName) mapper().getFull(datasetKey, "u102");
    assertEquals("u102", n.getId());
    assertEquals(Rank.SUBSPECIES, n.getRank());
    assertEquals("Chromis (Chromis) agilis pacifica", n.getName());
    assertEquals("<i>Chromis (Chromis) agilis pacifica</i> Smith, 1973", n.getNameHtml());
    assertEquals("Smith, 1973", n.getAuthor());
    assertEquals("Chromis", n.getGenus());
    assertEquals("Chromis", n.getSubgenus());
    assertEquals("agilis", n.getSpecies());
    assertEquals("pacifica", n.getInfraspecies());
    assertEquals("subsp.", n.getInfraspeciesMarker());
  }

  void assertPresent(LHigherName hn){
    assertNotNull(hn.getId());
    assertNotNull(hn.getRank());
    assertNotNull(hn.getName());
    assertNotNull(hn.getStatus());
  }
  @Test
  @Ignore
  public void count() {
    // Apia apis
    // Malus sylvestris
    // Larus fuscus
    // Larus fusca
    // Larus erfundus
    assertEquals(3, mapper().count(datasetKey, true, "Larus"));
    assertEquals(3, mapper().count(datasetKey, true, "larus"));
    assertEquals(0, mapper().count(datasetKey, false, "Larus"));
    assertEquals(1, mapper().count(datasetKey, false, "Larus fusca"));
    assertEquals(2, mapper().count(datasetKey, true, "Larus fusc"));
    assertEquals(0, mapper().count(datasetKey, true, "fusc"));
  }

  @Test
  public void search() {
    mapper().search(datasetKey, false, "Larus" ,0 ,2).forEach(this::isSpecies);
    mapper().searchFull(datasetKey, false, "Larus" ,0 ,2).forEach(this::isSpecies);
  }

  LSpeciesName isSpecies(LName n) {
    assertEquals(LSpeciesName.class, n.getClass());
    return (LSpeciesName) n;
  }
}