package life.catalogue.importer;

import life.catalogue.api.model.DatasetWithSettings;
import life.catalogue.api.model.User;
import life.catalogue.api.vocab.DataFormat;
import life.catalogue.api.vocab.DatasetOrigin;
import life.catalogue.api.vocab.DatasetType;
import life.catalogue.config.ImporterConfig;
import life.catalogue.config.NormalizerConfig;
import life.catalogue.dao.DatasetDao;
import life.catalogue.db.PgSetupRule;
import life.catalogue.db.mapper.UserMapper;
import life.catalogue.es.NameUsageIndexService;
import life.catalogue.img.ImageService;
import life.catalogue.importer.neo.NeoDb;
import life.catalogue.importer.neo.NeoDbFactory;
import life.catalogue.matching.NameIndexFactory;

import org.gbif.nameparser.api.NomCode;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import javax.validation.Validation;
import javax.validation.Validator;

import org.apache.commons.io.FileUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * Imports the given datasets from the test resources
 * into postgres.
 *
 * Requires a running postgres instance which is normally provided via the PgSetupRule ClassRule.
 */
public class PgImportRule extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(PgImportRule.class);
  
  static final User IMPORT_USER = new User();
  static {
    IMPORT_USER.setUsername("importator");
    IMPORT_USER.setFirstname("Tim");
    IMPORT_USER.setLastname("Tester");
    IMPORT_USER.setEmail("tim.test@mailinator.com");
    IMPORT_USER.getRoles().add(User.Role.ADMIN);
  }

  private NeoDb store;
  private NormalizerConfig cfg;
  private ImporterConfig icfg = new ImporterConfig();
  private DatasetWithSettings dataset;
  private final TestResource[] datasets;
  private final Map<TestResource, Integer> datasetKeyMap = new HashMap<>();
  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  public static PgImportRule create(Object... params) {
    List<TestResource> resources = new ArrayList<>();
    DataFormat format = null;
    NomCode code = null;
    DatasetType type = DatasetType.OTHER;
    for (Object p : params) {
      if (p instanceof DataFormat) {
        format = (DataFormat) p;
      } else if (p instanceof NomCode) {
        code = (NomCode) p;
      } else if (p instanceof DatasetType) {
        type = (DatasetType) p;
      } else if (p instanceof Integer) {
        resources.add(new TestResource((Integer)p, Preconditions.checkNotNull(format), code, type));
      }
    }
    return new PgImportRule(resources.toArray(new TestResource[0]));
  }

  public PgImportRule(TestResource... datasets) {
    this.datasets = datasets;
  }
  
  public static class TestResource {
    public final int key;
    public final DataFormat format;
    public final NomCode code;
    public final DatasetType type;
  
    private TestResource(int key, DataFormat format, NomCode code, DatasetType type) {
      this.key = key;
      this.format = Preconditions.checkNotNull(format);
      this.code = code;
      this.type = type;
    }
  
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestResource that = (TestResource) o;
      return key == that.key && format == that.format;
    }
  
    @Override
    public int hashCode() {
      return Objects.hash(key, format);
    }
  }
  
  @Override
  protected void before() throws Throwable {
    LOG.info("run PgImportRule with {} datasets", datasets.length);
    super.before();
  
    cfg = new NormalizerConfig();
    cfg.archiveDir = Files.createTempDir();
    cfg.scratchDir = Files.createTempDir();
    try (SqlSession session = PgSetupRule.getSqlSessionFactory().openSession(true)) {
      session.getMapper(UserMapper.class).create(IMPORT_USER);
    }
  
    for (TestResource tr : datasets) {
      normalizeAndImport(tr);
      datasetKeyMap.put(tr, dataset.getKey());
    }
  }
  
  @Override
  protected void after() {
    super.after();
    if (store != null) {
      store.closeAndDelete();
      FileUtils.deleteQuietly(cfg.archiveDir);
      FileUtils.deleteQuietly(cfg.scratchDir);
    }
  }
  
  public Integer datasetKey(int key, DataFormat format) {
    return datasetKeyMap.get(new TestResource(key, format, null, null));
  }

  void normalizeAndImport(TestResource tr) throws Exception {
    URL url = getClass().getResource("/" + tr.format.name().toLowerCase() + "/" + tr.key);
    Path source = Paths.get(url.toURI());
    dataset = new DatasetWithSettings();
    dataset.setCreatedBy(IMPORT_USER.getKey());
    dataset.setModifiedBy(IMPORT_USER.getKey());
    dataset.setDataFormat(tr.format);
    dataset.setType(tr.type);
    dataset.setOrigin(DatasetOrigin.MANAGED);
    dataset.setCode(tr.code);
    dataset.setTitle("Test Dataset " + source.toString());

    Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
    DatasetDao ddao = new DatasetDao(PgSetupRule.getSqlSessionFactory(), null, null, validator);
    // insert trusted dataset
    // this creates a new key, usually above 1000!
    ddao.create(dataset, IMPORT_USER.getKey());

    // normalize
    store = NeoDbFactory.create(dataset.getKey(), 1, cfg);
    Normalizer norm = new Normalizer(dataset, store, source, NameIndexFactory.passThru(), ImageService.passThru(), validator);
    norm.call();

    // import into postgres
    store = NeoDbFactory.open(dataset.getKey(), 1, cfg);
    PgImport importer = new PgImport(1, dataset, IMPORT_USER.getKey(), store, PgSetupRule.getSqlSessionFactory(), icfg, ddao, NameUsageIndexService.passThru(), validator);
    importer.call();
  }
  
}
