package life.catalogue.common.kryo;

import life.catalogue.api.TestEntityGenerator;
import life.catalogue.api.model.*;
import life.catalogue.api.search.NameUsageWrapper;
import life.catalogue.api.vocab.*;
import life.catalogue.common.date.FuzzyDate;

import org.gbif.dwc.terms.*;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ApiKryoPoolTest {
  Kryo kryo = new ApiKryoPool(1).create();
  
  @Test
  public void testName() throws Exception {
    Name n = TestEntityGenerator.newName("1234567");
    assertSerde(n);
  }
  
  @Test
  public void testReference() throws Exception {
    Reference r = new Reference();
    r.setId("1234");
    r.setYear(1984);
    r.setDatasetKey(77);
    r.setCsl(TestEntityGenerator.createCsl());
    assertSerde(r);
  }

  @Test
  public void testAreas() throws Exception {
    var d = new Distribution();
    d.setId(1234);
    d.setReferenceId("1984");
    d.setStatus(DistributionStatus.NATIVE);
    d.setSectorKey(13);
    d.setVerbatimKey(6789);
    d.setArea(new AreaImpl("Berlin"));
    assertSerde(d);

    d.setArea(Country.ALBANIA);
    assertSerde(d);

    d.setArea(TdwgArea.of("BZN"));
    assertSerde(d);
  }

  @Test
  public void testDataset() throws Exception {
    Dataset d = TestEntityGenerator.newDataset("Unmut");
    d.setKey(1234);
    d.setIssued(FuzzyDate.now());
    d.setGbifKey(UUID.randomUUID());
    d.setCreator(Agent.parse(List.of("Karl", "Frank")));
    d.setEditor(Agent.parse(List.of("Karlo", "Franko")));
    assertSerde(d);
  }

  @Test
  public void testUsages() throws Exception {
    Taxon t = TestEntityGenerator.newTaxon("bla bla");
    assertSerde(t);

    Synonym s = TestEntityGenerator.newSynonym(t);
    assertSerde(s);
  }

  @Test
  public void testUsageWrappers() throws Exception {
    NameUsageWrapper nuw = TestEntityGenerator.newNameUsageSynonymWrapper();
    assertSerde(nuw);

    nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    assertSerde(nuw);

    nuw = TestEntityGenerator.newNameUsageBareNameWrapper();
    assertSerde(nuw);
  }

  @Test
  public void testVerbatim() throws Exception {
    List<Term> terms = Lists.newArrayList(
        DwcTerm.scientificName, DwcTerm.associatedOrganisms, DwcTerm.taxonID,
        DcTerm.title,
        GbifTerm.canonicalName,
        IucnTerm.threatStatus,
        AcefTerm.Family,
        UnknownTerm.build("http://gbif.org/abcdefg")
    );
    assertSerde(terms);

    VerbatimRecord rec = TestEntityGenerator.createVerbatim();
    for (Issue issue : Issue.values()) {
      rec.addIssue(issue);
    }
    assertSerde(rec);
  }

  @Test
  public void testEmptyModels() throws Exception {
    assertSerde(new Taxon());
    assertSerde(new Name());
    assertSerde(new Reference());
    assertSerde(new Dataset());
    assertSerde(new DatasetImport());
  }

  private void assertSerde(Object obj) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
    Output output = new Output(buffer);
    kryo.writeObject(output, obj);
    output.close();
    byte[] bytes = buffer.toByteArray();

    final Input input = new Input(bytes);
    Object obj2 = kryo.readObject(input, obj.getClass());

    assertEquals(obj, obj2);
  }

}