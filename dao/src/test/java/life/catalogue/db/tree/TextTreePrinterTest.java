package life.catalogue.db.tree;

import life.catalogue.api.model.DSID;
import life.catalogue.common.io.Resources;
import life.catalogue.dao.TaxonCounter;
import life.catalogue.db.PgSetupRule;
import life.catalogue.db.TestDataRule;

import org.gbif.nameparser.api.Rank;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TextTreePrinterTest {
  
  @ClassRule
  public static PgSetupRule pgSetupRule = new PgSetupRule();
  
  @Rule
  public final TestDataRule testDataRule = TestDataRule.tree2();

  @Test
  public void print() throws IOException {
    Writer writer = new StringWriter();
    int count = PrinterFactory.dataset(TextTreePrinter.class, TestDataRule.TREE.key, PgSetupRule.getSqlSessionFactory(), writer).print();
    assertEquals(25, count);
    String expected = IOUtils.toString(Resources.stream("trees/tree2.tree"), StandardCharsets.UTF_8);
    assertEquals(expected, writer.toString());
  }

  @Test
  public void printWithCounts() throws IOException {
    Writer writer = new StringWriter();
    AtomicInteger cnt = new AtomicInteger(1);
    TaxonCounter counter = new TaxonCounter() {
      @Override
      public int count(DSID<String> taxonID, Rank countRank) {
        return cnt.getAndIncrement();
      }
    };
    var p = PrinterFactory.dataset(TextTreePrinter.class, TestDataRule.TREE.key, null, false, Set.of(Rank.FAMILY, Rank.GENUS), Rank.SPECIES, counter, PgSetupRule.getSqlSessionFactory(), writer);
    p.showIDs();
    int count = p.print();
    System.out.println(writer);
    assertEquals(5, count);
    String expected = IOUtils.toString(Resources.stream("trees/treeWithCounts.tree"), StandardCharsets.UTF_8);
    assertEquals(expected, writer.toString());
  }

}