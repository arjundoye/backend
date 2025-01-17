package life.catalogue.postgres;

import life.catalogue.api.model.Name;
import life.catalogue.common.tax.AuthorshipNormalizer;

import com.google.common.collect.Lists;

public class AuthorshipNormFunc {
  private final int startIdx;
  
  public AuthorshipNormFunc(int startIdx) {
    this.startIdx = startIdx;
  }

  public String normAuthorship (String[] row) {
    return normAuthorship(startIdx, row);
  }

  /**
   * @param startIdx column index of the name.csv that contains the basionym author
   * @param row
   */
  public static String normAuthorship (int startIdx, String[] row) {
    Name n = new Name();
    n.getBasionymAuthorship().setAuthors(Lists.newArrayList(PgCopyUtils.splitPgArray(row[startIdx])));
    n.getBasionymAuthorship().setExAuthors(Lists.newArrayList(PgCopyUtils.splitPgArray(row[startIdx+1])));
    n.getBasionymAuthorship().setYear(row[startIdx+2]);
    n.getCombinationAuthorship().setAuthors(Lists.newArrayList(PgCopyUtils.splitPgArray(row[startIdx+3])));
    n.getCombinationAuthorship().setExAuthors(Lists.newArrayList(PgCopyUtils.splitPgArray(row[startIdx+4])));
    n.getCombinationAuthorship().setYear(row[startIdx+5]);
    return PgCopyUtils.buildPgArray( AuthorshipNormalizer.INSTANCE.normalizeName(n).toArray(new String[0]) );
  }
}

