package life.catalogue.db.mapper;

import life.catalogue.api.RandomUtils;
import life.catalogue.api.TestEntityGenerator;
import life.catalogue.api.model.*;
import life.catalogue.api.vocab.Users;
import life.catalogue.dao.NameDao;
import life.catalogue.db.PgSetupRule;
import life.catalogue.db.TestDataRule;
import life.catalogue.es.NameUsageIndexService;
import life.catalogue.matching.NameIndexFactory;

import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.nameparser.api.Rank;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ibatis.cursor.Cursor;
import org.junit.Before;
import org.junit.Test;

import static life.catalogue.api.TestEntityGenerator.DATASET11;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NameUsageMapperTest extends MapperTestBase<NameUsageMapper> {
  
  TaxonMapper tm;
  SynonymMapper sm;
  NameMapper nm;
  int idGen = 1;
  int datasetKey = TestDataRule.APPLE.key;

  public NameUsageMapperTest() {
    super(NameUsageMapper.class, TestDataRule.apple());
  }
  
  @Before
  public void init() {
    tm = mapper(TaxonMapper.class);
    sm = mapper(SynonymMapper.class);
    nm = mapper(NameMapper.class);
  }

  @Test
  public void copyDataset() throws Exception {
    CopyDatasetTestComponent.copy(mapper(), testDataRule.testData.key, true);
  }

  @Test
  public void processDataset() throws Exception {
    assertSize(mapper().processDataset(testDataRule.testData.key, null, null), 4);
    assertSize(mapper().processDataset(testDataRule.testData.key, Rank.SPECIES, Rank.GENUS), 4);
    assertSize(mapper().processDataset(testDataRule.testData.key, Rank.SUBGENUS, Rank.GENUS), 0);
    assertSize(mapper().processDataset(testDataRule.testData.key, Rank.VARIETY, Rank.SPECIES), 4);
  }

  @Test
  public void processDatasetBareNames() throws Exception {
    assertSize(mapper().processDatasetBareNames(testDataRule.testData.key, null, null), 1);
    assertSize(mapper().processDatasetBareNames(testDataRule.testData.key, Rank.SPECIES, Rank.GENUS), 1);
    assertSize(mapper().processDatasetBareNames(testDataRule.testData.key, Rank.SUBGENUS, Rank.GENUS), 0);
    assertSize(mapper().processDatasetBareNames(testDataRule.testData.key, Rank.VARIETY, Rank.SPECIES), 1);
  }

  static void assertSize(Cursor<?> cursor, int size) {
    final AtomicInteger cnt = new AtomicInteger(0);
    cursor.forEach(u -> {
      cnt.incrementAndGet();
    });
    assertEquals(size, cnt.get());
  }

  @Test
  public void sectorProcessable() throws Exception {
    SectorProcessableTestComponent.test(mapper(), DSID.of(testDataRule.testData.key, 1));
  }

  @Test
  public void getSimple() throws Exception {
    Taxon t = TestEntityGenerator.TAXON1;
    SimpleName sn = mapper().getSimple(t);
    assertEquals(t.getId(), sn.getId());
    assertEquals(t.getParentId(), sn.getParent());
    assertEquals(t.getStatus(), sn.getStatus());
    assertEquals(t.getName().getRank(), sn.getRank());
    assertEquals(t.getName().getScientificName(), sn.getName());
    assertEquals(t.getName().getAuthorship(), sn.getAuthorship());
  }

  @Test
  public void listRelated() throws Exception {
    var results = mapper().listRelated(DSID.of(testDataRule.testData.key, "root-2"), null, null);
    assertEquals(1, results.size());
    assertEquals("s1", results.get(0).getId());

    results = mapper().listRelated(DSID.of(testDataRule.testData.key, "root-2"), List.of(testDataRule.testData.key), null);
    assertEquals(1, results.size());
    assertEquals("s1", results.get(0).getId());

    assertTrue(mapper().listRelated(DSID.of(testDataRule.testData.key, "root-2"), null, UUID.randomUUID()).isEmpty());
    assertTrue(mapper().listRelated(DSID.of(testDataRule.testData.key, "root-2"), List.of(1,2,3), UUID.randomUUID()).isEmpty());
    assertTrue(mapper().listRelated(DSID.of(testDataRule.testData.key, "root-2"), List.of(1,2,3), null).isEmpty());
  }

  @Test
  public void listByRegex() throws Exception {
    List<SimpleName> res = mapper().listByRegex(testDataRule.testData.key, ".", null, null, new Page());
    assertEquals(4, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, ".", TaxonomicStatus.ACCEPTED, null, new Page());
    assertEquals(2, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, ".", TaxonomicStatus.ACCEPTED, Rank.GENUS, new Page());
    assertEquals(0, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, ".*fu[ns]", null, null, new Page());
    assertEquals(3, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, ".*fus", null, null, new Page());
    assertEquals(2, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, "La", null, null, new Page());
    assertEquals(3, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, ".+ .+tris$", null, null, new Page());
    assertEquals(1, res.size());

    res = mapper().listByRegex(testDataRule.testData.key, "[A-Za-z]+\\s", null, null, new Page());
    assertEquals(4, res.size());
  }

  @Test
  public void list() throws Exception {
    NameDao nameDao = new NameDao(PgSetupRule.getSqlSessionFactory(), NameUsageIndexService.passThru(), NameIndexFactory.passThru(), validator);
  
    List<Taxon> taxa = new ArrayList<>();
    taxa.add(TestEntityGenerator.newTaxon("t1"));
    taxa.add(TestEntityGenerator.newTaxon("t2"));
    taxa.add(TestEntityGenerator.newTaxon("t3"));
    taxa.add(TestEntityGenerator.newTaxon("t4"));
    taxa.add(TestEntityGenerator.newTaxon("t5"));
    taxa.add(TestEntityGenerator.newTaxon("t6"));
    taxa.add(TestEntityGenerator.newTaxon("t7"));
    taxa.add(TestEntityGenerator.newTaxon("t8"));
    taxa.add(TestEntityGenerator.newTaxon("t9"));
    for (Taxon t : taxa) {
      tm.create(t);
    }
  
    List<Synonym> syns = new ArrayList<>();
    syns.add(TestEntityGenerator.newSynonym(taxa.get(2)));
    syns.add(TestEntityGenerator.newSynonym(taxa.get(2)));
    syns.add(TestEntityGenerator.newSynonym(taxa.get(5)));
    for (Synonym s : syns) {
      nameDao.create(s.getName(), Users.TESTER);
      sm.create(s);
    }
    commit();
    
    // get first page
    Page p = new Page(0, 3);
    
    List<NameUsageBase> res = mapper().list(DATASET11.getKey(), p);
    assertEquals(3, res.size());
    // First 2 taxa in dataset 11 are pre-inserted taxa
    // next 2 are preinserted synonyms
    // then our 3 created syns
    // finally 9 new taxa
    assertIdClassEquals(TestEntityGenerator.TAXON1, res.get(0));
    assertIdClassEquals(TestEntityGenerator.TAXON2, res.get(1));
    assertIdClassEquals(TestEntityGenerator.SYN1, res.get(2));

    p.next();
    res = mapper().list(DATASET11.getKey(), p);
    assertEquals(3, res.size());
    assertIdClassEquals(TestEntityGenerator.SYN2, res.get(0));
    assertIdClassEquals(syns.get(0), res.get(1));
    assertIdClassEquals(syns.get(1), res.get(2));
  
    p.next();
    res = mapper().list(DATASET11.getKey(), p);
    assertEquals(3, res.size());
    assertIdClassEquals(syns.get(2), res.get(0));
    assertIdClassEquals(taxa.get(0), res.get(1));
    assertIdClassEquals(taxa.get(1), res.get(2));
  }


  private Name createName(Rank rank, DSID<Integer> sectorKey) {
    Name n = TestEntityGenerator.newName(sectorKey.getDatasetKey(), "n-" + idGen++,
      rank.isSpeciesOrBelow() ? RandomUtils.randomSpecies() : RandomUtils.randomGenus(), rank);
    n.setSectorKey(sectorKey.getId());
    nm.create(n);
    return n;
  }

  private Taxon createTaxon(Rank rank, String parentID, DSID<Integer> sectorKey){
    Name n = createName(rank, sectorKey);
    Taxon t = TestEntityGenerator.newTaxon(n, "t-"+idGen++, parentID);
    t.setSectorKey(sectorKey.getId());
    t.setDatasetKey(sectorKey.getDatasetKey());
    tm.create(t);
    return t;
  }

  private Synonym createSynonym(Taxon acc, Rank rank){
    DSID<Integer> secKey = DSID.of(acc.getDatasetKey(), acc.getSectorKey());
    Name n = createName(rank, secKey);
    Synonym s = TestEntityGenerator.newSynonym(n, acc.getId());
    s.setSectorKey(acc.getSectorKey());
    sm.create(s);
    return s;
  }

  @Test
  public void deleteBySectorAndRank() throws Exception {
    // no real data to delete but tests valid SQL
    mapper().createTempTable();
    mapper().addSectorBelowRankToTemp(DSID.of(datasetKey, 1), Rank.GENUS);
    mapper().addSectorBelowRankToTemp(DSID.of(datasetKey, 1), Rank.SUPERSECTION);
    mapper().addSectorBelowRankToTemp(DSID.of(datasetKey, 1), Rank.FAMILY);
  }

  @Test
  public void deleteUsages() throws Exception {
    // attach taxa with sector to
    final var s1 = SectorMapperTest.create(DSID.of(datasetKey, "aha"), DSID.of(datasetKey, "bha"));
    mapper(SectorMapper.class).create(s1);
    commit();
    for (int sp = 1; sp < 10; sp++) {
      Taxon species = createTaxon(Rank.SPECIES, "root-1", s1);
      createTaxon(Rank.UNRANKED, species.getId(), s1);
      createTaxon(Rank.VARIETY, species.getId(), s1);
      createSynonym(species, Rank.SPECIES_AGGREGATE);
      for (int ssp = 1; ssp < 5; ssp++) {
        Taxon subspecies = createTaxon(Rank.SUBSPECIES, species.getId(), s1);
        createTaxon(Rank.UNRANKED, subspecies.getId(), s1);
        for (int var = 1; var < 3; var++) {
          Taxon variety = createTaxon(Rank.VARIETY, species.getId(), s1);
          createTaxon(Rank.CHEMOFORM, variety.getId(), s1);
        }
        createTaxon(Rank.VARIETY, subspecies.getId(), s1);
        createSynonym(subspecies, Rank.UNRANKED);
        for (int syn = 1; syn < 5; syn++) {
          createSynonym(subspecies, Rank.VARIETY);
        }
      }
    }
    commit();

    final AtomicInteger count = new AtomicInteger(0);
    mapper().processSector(s1).forEach(n -> count.incrementAndGet());
    assertEquals(468, count.get());

    // delete
    mapper().createTempTable();
    mapper().addSectorBelowRankToTemp(s1, Rank.SUBSPECIES);
    int dels = mapper().deleteByTemp(s1.getDatasetKey());
    assertEquals(450, dels);
    count.set(0);
    mapper().processSector(s1).forEach(n -> count.incrementAndGet());
    assertEquals(18, count.get());
  }

  void assertIdClassEquals(NameUsageBase o1, NameUsageBase o2) {
    assertEquals(o1.getId(), o2.getId());
    assertEquals(o1.getClass(), o2.getClass());
  }
}