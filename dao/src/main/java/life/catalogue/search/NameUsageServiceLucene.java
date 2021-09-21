package life.catalogue.search;

import life.catalogue.api.model.DSID;
import life.catalogue.api.model.Page;
import life.catalogue.api.search.*;
import life.catalogue.common.io.PathUtils;
import life.catalogue.common.util.LoggingUtils;
import life.catalogue.dao.DaoUtils;
import life.catalogue.dao.NameUsageProcessor;
import life.catalogue.db.mapper.NameUsageWrapperMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameUsageServiceLucene implements NameUsageIndexService, NameUsageSearchService, NameUsageSuggestionService{
  private static final Logger LOG = LoggerFactory.getLogger(NameUsageServiceLucene.class);
  private final LuceneConfig cfg;
  private final SqlSessionFactory factory;
  private NameUsageIndex index;

  public NameUsageServiceLucene(LuceneConfig cfg, SqlSessionFactory factory) {
    this.cfg = cfg;
    this.factory = factory;
    index = new NameUsageIndex(cfg);
  }

  @Override
  public void createEmptyIndex() {
    try {
      index.close();
      if (cfg.directory != null) {
        PathUtils.deleteRecursively(cfg.directory);
      }
      index = new NameUsageIndex(cfg);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stats indexSector(DSID<Integer> sectorKey) {
    return null;
  }

  @Override
  public void deleteSector(DSID<Integer> sectorKey) {

  }

  @Override
  public int deleteBareNames(int datasetKey) {
    return 0;
  }

  @Override
  public void deleteSubtree(DSID<String> root) {

  }

  @Override
  public Stats indexDataset(int datasetKey) {
    return indexDataset(datasetKey, true);
  }

  private Stats indexDataset(int datasetKey, boolean clearIndex) {
    Stats stats = new Stats();

    try (SqlSession lockSession = factory.openSession()) {
      LoggingUtils.setDatasetMDC(datasetKey, getClass());
      LOG.info("Start indexing dataset {}", datasetKey);
      // we lock the main dataset tables so they are only accessible by select statements, but not any modifying statements.
      DaoUtils.aquireTableLock(datasetKey, lockSession);
      if (clearIndex) {
        LOG.info("Remove dataset {} from index", datasetKey);
        //TODO
      }
      LOG.info("Indexing usages from dataset {}", datasetKey);
      NameUsageProcessor processor = new NameUsageProcessor(factory);
      final AtomicInteger counter = new AtomicInteger();
      processor.processDataset(datasetKey, nuw -> {
        index.add(nuw);
        counter.incrementAndGet();
      });
      stats.usages = counter.get();

      try (SqlSession session = factory.openSession()) {
        counter.set(0);
        LOG.info("Indexing bare names from dataset {}", datasetKey);
        NameUsageWrapperMapper mapper = session.getMapper(NameUsageWrapperMapper.class);
        mapper.processDatasetBareNames(datasetKey, null).forEach(u -> {
          index.add(u);
          counter.incrementAndGet();
        });
        stats.names = counter.get();
      }

      index.commit();
      LOG.info("Successfully indexed dataset {} into the search index. Usages: {}. Bare names: {}. Total: {}.",
        datasetKey, stats.usages, stats.names, stats.total());

    } finally {
      LoggingUtils.removeDatasetMDC();
    }
    return stats;
  }

  @Override
  public int deleteDataset(int datasetKey) {
    return 0;
  }

  @Override
  public Stats indexAll() {
    return null;
  }

  @Override
  public void delete(DSID<String> usageId) {

  }

  @Override
  public void update(int datasetKey, Collection<String> taxonIds) {

  }

  @Override
  public void add(NameUsageWrapper usage) {
    index.add(usage);
  }

  @Override
  public void updateClassification(int datasetKey, String rootTaxonId) {

  }

  @Override
  public NameUsageSearchResponse search(NameUsageSearchRequest req, Page page) {
    try {
      QueryParser parser = new QueryParser("sciname", index.writer.getAnalyzer());
      Query q = parser.parse(req.getQ());
      var result = index.search(q, page);
      NameUsageSearchResponse resp = new NameUsageSearchResponse(page, result.size(), result);
      return resp;

    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NameUsageSuggestResponse suggest(NameUsageSuggestRequest request) {
    return null;
  }
}
