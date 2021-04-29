package life.catalogue.exporter;

import life.catalogue.ApiUtils;
import life.catalogue.WsServerConfig;
import life.catalogue.api.vocab.Users;
import life.catalogue.db.PgSetupRule;
import life.catalogue.db.TestDataRule;
import life.catalogue.img.ImageService;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DwcaExporterTest extends ExporterTest {
  WsServerConfig cfg;

  @Before
  public void initCfg()  {
    cfg = new WsServerConfig();
    cfg.apiURI=ApiUtils.API;
    cfg.exportDir = dir;
  }

  @Test
  public void dataset() {
    DwcaExporter exp = new DwcaExporter(ExportRequest.dataset(TestDataRule.APPLE.key, Users.TESTER), PgSetupRule.getSqlSessionFactory(), cfg, ImageService.passThru());
    exp.run();

    assertTrue(exp.getArchive().exists());
  }

}