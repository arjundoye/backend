package life.catalogue.search;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.base.Preconditions;

import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneConfig {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneConfig.class);

  public Path directory;
  public File kvpStore;

  public FSDirectory indexDir() throws IOException {
    Preconditions.checkNotNull(directory, "No index directory configured");
    if (Files.isDirectory(directory)) {
      LOG.info("Loading existing search index from disk: {}", directory);
    } else {
      LOG.info("Create new search index on disk: {}", directory);
    }
    return FSDirectory.open(directory);
  }

}
