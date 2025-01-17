package life.catalogue.es.nu;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Focuses on the normalization methods within NameUsageWrapperConverter
 *
 */
public class NameUsageWrapperConverterTest {

  @Test
  public void testNormalizeWeakly1() {
    String s = NameUsageWrapperConverter.normalizeWeakly("Larus");
    assertEquals("larus", s);
  }

  @Test
  public void testNormalizeWeakly2() {
    String s = NameUsageWrapperConverter.normalizeWeakly("等待");
    assertEquals("等待", s);
  }

  @Test
  public void testNormalizeWeakly3() {
    String s = NameUsageWrapperConverter.normalizeWeakly("sérieux");
    assertEquals("serieux", s);
  }

  @Test
  public void testNormalizeStrongly1a() {
    String s = NameUsageWrapperConverter.normalizeStrongly("Larus");
    System.out.println(s);
    assertEquals("lar", s);
  }

  @Test
  public void testNormalizeStrongly1b() {
    String s = NameUsageWrapperConverter.normalizeStrongly("Larus fuscus");
    assertEquals("larus fusc", s);
  }

  @Test
  public void testNormalizeStrongly1c() {
    String s = NameUsageWrapperConverter.normalizeStrongly("Larus fuscus fuscus");
    System.out.println(s);
    assertEquals("larus fuscus fusc", s);
  }

  @Test
  public void testNormalizeStrongly2() {
    String s = NameUsageWrapperConverter.normalizeStrongly("等待");
    assertEquals("等待", s);
  }

  @Test
  public void testNormalizeStrongly3() {
    String s = NameUsageWrapperConverter.normalizeStrongly("sérieux");
    assertEquals("serieux", s);
  }

  @Test
  public void testNormalizeStrongly4() {
    String s = NameUsageWrapperConverter.normalizeStrongly("sylvestris");
    System.out.println(s);
    assertEquals("silvestr", s);
  }

}
