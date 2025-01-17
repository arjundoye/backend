package life.catalogue.common.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Known to fail on Windows - which we gladly do not use!
*/
public class ChecksumUtilsTest {
  
  @Test
  public void getMD5Checksum() throws Exception {
    assertEquals("A36FA440FCB635530C612C8DDE0002E5", ChecksumUtils.getMD5Checksum(PathUtils.classPathTestFile("charsets/cp1252.txt")));
  }
  
  @Test
  public void getSHAChecksum() throws Exception {
    assertEquals("BD2C10814FE4956B4CDCE53E65A10CADD7DE03BC", ChecksumUtils.getSHAChecksum(PathUtils.classPathTestFile("charsets/cp1252.txt")));
  }
}