package life.catalogue.portal;

import life.catalogue.api.exception.NotFoundException;
import life.catalogue.api.exception.SynonymException;
import life.catalogue.api.model.*;
import life.catalogue.api.vocab.Datasets;
import life.catalogue.cache.LatestDatasetKeyCache;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.dao.DatasetSourceDao;
import life.catalogue.dao.TaxonDao;
import life.catalogue.db.mapper.NameUsageMapper;
import life.catalogue.metadata.FmUtil;
import life.catalogue.resources.ResourceUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import freemarker.template.Template;
import freemarker.template.TemplateException;

import static life.catalogue.api.util.ObjectUtils.checkFound;

public class PortalPageRenderer {
  private static final Logger LOG = LoggerFactory.getLogger(PortalPageRenderer.class);

  public enum PortalPage {NOT_FOUND, TAXON, DATASET};
  public enum Environment {PROD, PREVIEW, DEV};

  private final DatasetSourceDao sourceDao;
  private final TaxonDao tdao;
  private final LatestDatasetKeyCache cache;
  private final Map<Environment, Map<PortalPage, Template>> portalTemplates = Map.copyOf(
    Arrays.stream(Environment.values())
          .collect(Collectors.toMap(e -> e, e -> new HashMap<>()))
  );
  private Path portalTemplateDir = Path.of("/tmp");

  public PortalPageRenderer(DatasetSourceDao sourceDao, TaxonDao tdao, LatestDatasetKeyCache cache, Path portalTemplateDir) throws IOException {
    this.sourceDao = sourceDao;
    this.tdao = tdao;
    this.cache = cache;
    setTemplateFolder(portalTemplateDir);
  }

  public Path getPortalTemplateDir() {
    return portalTemplateDir;
  }

  /**
   * @param id a COL checklist taxon or synonym ID. In case of synonyms redirect to the taxon page.
   * @param env
   */
  public Response renderTaxon(String id, Environment env) throws TemplateException, IOException {
    try {
      final int datasetKey = releaseKey(env);
      final Map<String, Object> data = buildData(datasetKey);

      final var key = new DSIDValue<>(datasetKey, id);
      //TODO: check if we had the id in a previous release...
      Taxon t = tdao.getOr404(key);
      TaxonInfo info = new TaxonInfo();
      data.put("info", info);
      info.setTaxon(t);
      try (SqlSession session = tdao.getFactory().openSession()) {
        tdao.fillTaxonInfo(session, info, null,
          true,
          true,
          false,
          true,
          false,
          false,
          false,
          true,
          false,
          false
        );
        // put source dataset title into the ID field so we can show a real title
        if (info.getSource() != null) {
          Dataset source = sourceDao.get(datasetKey, info.getSource().getSourceDatasetKey(), false);
          data.put("source", source);
        }
        if (info.getTaxon().getParentId() != null) {
          SimpleName parent = session.getMapper(NameUsageMapper.class).getSimple(DSID.of(datasetKey, info.getTaxon().getParentId()));
          data.put("parent", parent);
        }
        return render(env, PortalPage.TAXON, data);
      }

    } catch (SynonymException e) {
      // redirect to accepted taxon, e.g.
      // https://www.catalogueoflife.org/data/taxon/4LQWB to /data/taxon/4LQWC
      URI location = URI.create("/data/taxon/" + e.acceptedKey.getId());
      throw ResourceUtils.redirect(location);

    } catch (NotFoundException e) {
      return render(env, PortalPage.NOT_FOUND, new HashMap<>());
    }
  }

  public Response renderDatasource(int id, Environment env) throws TemplateException, IOException {
    try {
      final int datasetKey = releaseKey(env);
      final Map<String, Object> data = buildData(datasetKey);

      var d = checkFound(
        sourceDao.get(datasetKey, id, false),
        String.format("Datasource %s not found in release %s", id, datasetKey)
      );
      data.put("source", d);
      return render(env, PortalPage.DATASET, data);

    } catch (NotFoundException e) {
      return render(env, PortalPage.NOT_FOUND, new HashMap<>());
    }
  }

  private Map<String, Object> buildData(int datasetKey) {
    final Map<String, Object> data = new HashMap<>();
    data.put("releaseKey", datasetKey);
    return data;
  }

  private Response render(Environment env, PortalPage pp, Object data) throws TemplateException, IOException {
    var temp = portalTemplates.get(env).get(pp);
    Writer out = new StringWriter();
    temp.process(data, out);

    return Response
      .status(pp == PortalPage.NOT_FOUND ? Response.Status.NOT_FOUND : Response.Status.OK)
      .type(MediaType.TEXT_HTML)
      .entity(out.toString())
      .build();
  }

  public void setTemplateFolder(Path portalTemplateDir) throws IOException {
    this.portalTemplateDir = Preconditions.checkNotNull(portalTemplateDir);
    if (!Files.exists(portalTemplateDir)) {
      Files.createDirectories(portalTemplateDir);
    }
    for (Environment env : Environment.values()) {
      for (PortalPage pp : PortalPage.values()) {
        loadTemplate(env, pp);
      }
    }
  }

  private void loadTemplate(Environment env, PortalPage pp) throws IOException {
    var p = template(env, pp);
    InputStream in;
    if (Files.exists(p)) {
      LOG.info("Load {} {} portal template from {}", env, pp, p);
      in = Files.newInputStream(p);
    } else {
      LOG.info("Load {} portal template from resources", pp);
      in = getClass().getResourceAsStream("/freemarker-templates/portal/"+pp.name()+".ftl");
    }
    loadTemplate(env, pp, in);
  }

  private void loadTemplate(Environment env, PortalPage pp, InputStream stream) throws IOException {
    try (BufferedReader br = UTF8IoUtils.readerFromStream(stream)) {
      Template temp = new Template(pp.name(), br, FmUtil.FMK);
      portalTemplates.get(env).put(pp, temp);
    }
  }

  public boolean store(Environment env, PortalPage pp, String template) throws IOException {
    LOG.info("Store new portal page template {}", pp);
    try (Writer w = UTF8IoUtils.writerFromPath(template(env, pp))) {
      // enforce xhtml freemarker setting which we cannot keep in Jekyll
      w.write("<#ftl output_format=\"XHTML\">");
      IOUtils.write(template, w);
    }
    loadTemplate(env, pp);
    return true;
  }

  private Path template(Environment env, PortalPage pp) {
    return portalTemplateDir.resolve(Path.of(env.name(), pp.name()+".ftl"));
  }

  private int releaseKey(Environment env) {
    boolean preview = env == Environment.PREVIEW;
    Integer key = preview ? cache.getLatestReleaseCandidate(Datasets.COL) : cache.getLatestRelease(Datasets.COL);
    if (key == null) throw new NotFoundException("No COL" + (preview ? " preview" : "") + " release existing");
    return key;
  }
}
