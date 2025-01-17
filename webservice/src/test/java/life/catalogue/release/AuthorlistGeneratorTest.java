package life.catalogue.release;

import life.catalogue.api.model.Agent;
import life.catalogue.api.model.Dataset;
import life.catalogue.api.model.DatasetSettings;
import life.catalogue.api.vocab.Setting;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static life.catalogue.api.model.Agent.organisation;
import static life.catalogue.api.model.Agent.person;
import static org.junit.Assert.assertEquals;

public class AuthorlistGeneratorTest {

  @Test
  public void appendSourceAuthors() throws Exception {
    var gen = new AuthorlistGenerator();

    final Dataset proj = new Dataset();
    final Agent markus = person("Markus", "Döring", "hit@me.com", "1111-2222-3333-4444", "Drummer");
    proj.setCreator(List.of(markus));
    proj.setContributor(List.of(person("Frank", "Berril"), person("Brant", "Spar")));
    final var frankOrcid = "1234-5678-9999-0000";

    Dataset d = new Dataset(proj);

    List<Dataset> sources = new ArrayList<>();
    var s1 = new Dataset();
    s1.setCreator(List.of(person("F", "Berril", null, frankOrcid), person("Arri", "Rønsen"), organisation("FAO")));
    sources.add(s1);

    var s2 = new Dataset();
    s2.setCreator(List.of(person("Gerry", "Newman"), person("Arri", "Rønsen"), organisation("GBIF")));
    s2.setEditor(List.of(person("A.F.", "Beril", null, frankOrcid)));
    sources.add(s2);

    var ds = new DatasetSettings();
    ds.enable(Setting.RELEASE_ADD_SOURCE_AUTHORS);
    ds.enable(Setting.RELEASE_ADD_CONTRIBUTORS);
    gen.appendSourceAuthors(d, sources, ds);
    assertEquals(7, d.getCreator().size());
    assertEquals(markus, d.getCreator().get(0));
    assertEquals(0, d.getContributor().size());

    d = new Dataset(proj);
    ds.disable(Setting.RELEASE_ADD_CONTRIBUTORS);
    gen.appendSourceAuthors(d, sources, ds);
    assertEquals(6, d.getCreator().size());
    assertEquals(proj.getContributor().size(), d.getContributor().size());

    var s3 = new Dataset();
    s3.setCreator(List.of(person("Markus", "Döring", "markus@vegan.pork", null, "Vegan")));
    sources.add(s3);

    d = new Dataset(proj);
    gen.appendSourceAuthors(d, sources, ds);
    assertEquals(6, d.getCreator().size());

    assertEquals("Drummer; Vegan", d.getCreator().get(0).getNote());
  }

}