package life.catalogue.search;

import life.catalogue.api.model.Page;
import life.catalogue.api.search.NameUsageRequest;
import life.catalogue.api.search.NameUsageSearchRequest;
import life.catalogue.common.io.PathUtils;
import life.catalogue.db.PgSetupRule;

import life.catalogue.db.TestDataRule;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class NameUsageServiceLuceneTest {

  @ClassRule
  public static PgSetupRule pgSetupRule = new PgSetupRule();

  @Rule
  public final TestDataRule testDataRule = TestDataRule.fish();

  LuceneConfig cfg = new LuceneConfig();
  NameUsageServiceLucene service;

  @Before
  public void init() throws Exception {
    cfg.directory = Files.createTempDirectory("colsearch");
    cfg.kvpStore = new File(cfg.directory.toFile(), "mapdb");
    service = new NameUsageServiceLucene(cfg, PgSetupRule.getSqlSessionFactory());
  }

  @After
  public void cleanup() throws Exception {
    PathUtils.deleteRecursively(cfg.directory);
    FileUtils.deleteQuietly(cfg.kvpStore);
  }


  @Test
  public void search() {
    service.indexDataset(testDataRule.testData.key);
    NameUsageSearchRequest req = new NameUsageSearchRequest();
    req.setSearchType(NameUsageRequest.SearchType.WHOLE_WORDS);
    req.setQ("Chromis");

    var resp = service.search(req, new Page());
  }
}