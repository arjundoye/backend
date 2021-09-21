package life.catalogue.search;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.Pool;

import life.catalogue.api.model.Name;
import life.catalogue.api.model.NameUsage;
import life.catalogue.api.model.Page;
import life.catalogue.api.search.NameUsageWrapper;

import java.io.IOException;
import java.util.*;

import life.catalogue.common.kryo.ApiKryoPool;
import life.catalogue.common.kryo.map.MapDbObjectSerializer;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameUsageIndex implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(NameUsageIndex.class);
  private final Directory dir;
  private final PerFieldAnalyzerWrapper analyzer;
  private final Map<Integer, Map<String, NameUsageWrapper>> usageStores = new HashMap<>();
  private final Pool<Kryo> pool;
  private final DB mapDb;
  IndexWriter writer;

  NameUsageIndex(LuceneConfig cfg) {
    try {
      this.dir = cfg.indexDir();
      Map<String, Analyzer> analyzers = Map.of(
        "publisherKey", new KeywordAnalyzer() // UUID anaylzer
      );
      analyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), analyzers);
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      writer = new IndexWriter(dir, iwc);
      // creates initial index segment
      writer.commit();
      // KVP store for entire objects - we use lucene only for indexing!
      if (!cfg.kvpStore.exists()) {
        FileUtils.forceMkdirParent(cfg.kvpStore);
        LOG.info("Create search index KVP store at {}", cfg.kvpStore.getAbsolutePath());
      } else {
        LOG.info("Use search index KVP store at {}", cfg.kvpStore.getAbsolutePath());
      }
      mapDb = DBMaker
        .fileDB(cfg.kvpStore)
        .fileMmapEnableIfSupported()
        .make();
      pool = new ApiKryoPool(16);
    } catch (IOException e) {
      LOG.error("Failed to init search index", e);
      throw new RuntimeException("Failed to init search index", e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
    dir.close();
    mapDb.close();
  }

  public List<NameUsageWrapper> search(Query q, Page page) {
    try {
      IndexReader reader = DirectoryReader.open(writer);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopScoreDocCollector collector = TopScoreDocCollector.create(page.getLimitWithOffset(), 100000);
      searcher.search(q, collector);
      List<NameUsageWrapper> result = new ArrayList<>();
      for (ScoreDoc sc : collector.topDocs().scoreDocs) {
        result.add(fromDoc(reader.document(sc.doc)));
      }
      return result;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, NameUsageWrapper> getOrCreateKVP(Integer datasetKey) {
    return usageStores.computeIfAbsent(datasetKey, k -> {
      Map<String, NameUsageWrapper> usages = mapDb.hashMap("u"+k)
                                                .keySerializer(Serializer.STRING)
                                                .valueSerializer(new MapDbObjectSerializer(NameUsageWrapper.class, pool, 64))
                                                .createOrOpen();
      return usages;
    });
  }

  public void add(NameUsageWrapper nuw) {
    try {
      writer.addDocument(toDoc(nuw));
      getOrCreateKVP(nuw.getUsage().getDatasetKey())
        .put(nuw.getId(), nuw);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void commit() {
    try {
      writer.commit();
      mapDb.commit();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Document toDoc(NameUsageWrapper nuw) {

    Document doc = new Document();
    storeKeyword(doc, "datasetKey", nuw.getUsage().getDatasetKey());
    storeKeyword(doc, "id", nuw.getId());

    indexInt(doc, "sectorDatasetKey", nuw.getSectorDatasetKey());
    indexKeyword(doc, "publisherKey", nuw.getPublisherKey());
    indexKeywords(doc, "issues", nuw.getIssues());
    // TODO
    //List<SimpleDecision> decisions;

    NameUsage u = nuw.getUsage();

    Name n = u.getName();
    indexText(doc, "scientificName", n.getScientificName());
    indexText(doc, "authorship", n.getAuthorship());
    indexEnum(doc, "rank", n.getRank());

    return doc;
  }

  private Document indexText(Document doc, String field, String value) {
    if (value != null) {
      doc.add(new TextField(field, value, Field.Store.NO));
    }
    return doc;
  }
  private Document storeKeyword(Document doc, String field, Object value) {
    if (value != null) {
      doc.add(new StringField(field, value.toString(), Field.Store.YES));
    }
    return doc;
  }
  private Document indexKeyword(Document doc, String field, Object value) {
    if (value != null) {
      doc.add(new StringField(field, value.toString(), Field.Store.NO));
    }
    return doc;
  }
  private Document indexKeywords(Document doc, String field, Collection<?> values) {
    if (values != null && !values.isEmpty()) {
      for (Object val : values){
        doc.add(new StringField(field, val.toString(), Field.Store.NO));
      }
    }
    return doc;
  }
  private Document indexEnum(Document doc, String field, Enum<?> value) {
    if (value != null) {
      doc.add(new StringField(field, value.name(), Field.Store.NO));
    }
    return doc;
  }
  private Document indexInt(Document doc, String field, Integer value) {
    if (value != null) {
      doc.add(new StringField(field, value.toString(), Field.Store.NO));
    }
    return doc;
  }

  NameUsageWrapper fromDoc(Document doc) {
    int datasetKey = Integer.valueOf(doc.get("datasetKey"));
    String id = doc.get("id");
    return getOrCreateKVP(datasetKey).get(id);
  }
}
