package life.catalogue.dao;

import life.catalogue.api.vocab.DatasetOrigin;

import life.catalogue.db.PgSetupRule;

import life.catalogue.db.TestDataRule;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.util.Set;

import static org.junit.Assert.*;

public class PartitionerTest {

  @ClassRule
  public static PgSetupRule pgSetupRule = new PgSetupRule();

  @Rule
  public final TestDataRule testDataRule = TestDataRule.apple();


  @Test
  public void keys() throws Exception {
    try (Connection con = pgSetupRule.connect()) {
      Set<String> keys = Partitioner.partitionSuffices(con, null);
      assertEquals(Set.of("3","11", "mod1", "mod0"), keys); // 12 is external, so kept in default partition

      keys = Partitioner.partitionSuffices(con, DatasetOrigin.MANAGED);
      assertEquals(Set.of("3","11"), keys);

      keys = Partitioner.partitionSuffices(con, DatasetOrigin.EXTERNAL);
      assertEquals(Set.of("mod1", "mod0"), keys);

      keys = Partitioner.partitionSuffices(con, DatasetOrigin.RELEASED);
      assertEquals(Set.of(), keys);
    }
  }
}