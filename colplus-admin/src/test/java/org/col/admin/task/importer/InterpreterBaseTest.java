package org.col.admin.task.importer;

import org.col.admin.task.importer.reference.ReferenceFactory;
import org.col.api.model.Dataset;
import org.col.csl.CslParserMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InterpreterBaseTest {

  InterpreterBase inter = new InterpreterBase(new Dataset(), null, new ReferenceFactory(1, new CslParserMock()));

  @Test
  public void latinName() throws Exception {
    assertEquals("Abies", inter.latinName("Abies"));
    assertEquals("Döring", inter.latinName("Döring"));
    assertEquals("Bào wén dōng fāng tún", inter.latinName("Bào wén dōng fāng tún"));
    assertEquals("bào wén duō jì tún", inter.latinName("豹紋多紀魨"));
  }

  @Test
  public void asciiName() throws Exception {
    assertEquals("Abies", inter.asciiName("Abiés"));
    assertEquals("Döring", inter.latinName("Döring"));
    assertEquals("Bao wen dong fang tun", inter.asciiName("Bào wén dōng fāng tún"));
    assertEquals("bao wen duo ji tun", inter.asciiName("豹紋多紀魨"));
  }

}
