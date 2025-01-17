package life.catalogue.csv;

import life.catalogue.common.io.PathUtils;

import org.gbif.dwc.terms.Term;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 *
 */
public class Schema {
  public final Path file;
  public final Term rowType;
  public final Charset encoding;
  public final CsvParserSettings settings;
  public final List<Field> columns;
  
  public Schema(Path file, Term rowType, Charset encoding, CsvParserSettings settings, List<Field> columns) {
    this.file = Preconditions.checkNotNull(file);
    this.rowType = Preconditions.checkNotNull(rowType);
    this.encoding = Preconditions.checkNotNull(encoding);
    this.settings = Preconditions.checkNotNull(settings);
    this.columns = ImmutableList.copyOf(columns);
  }
  
  public static class Field {
    public final Term term;
    public final String value;
    public final Integer index;
    public final String delimiter;
    
    public Field(Term term, Integer index) {
      this(term, null, index, null);
    }
    
    public Field(Term term, String value, Integer index, String delimiter) {
      this.term = Preconditions.checkNotNull(term);
      if (value == null && index == null) {
        throw new IllegalArgumentException("Default value or column index is required");
      }
      Preconditions.checkArgument(index == null || index >= 0, "Column index must be positive");
      this.value = value;
      this.index = index;
      this.delimiter = delimiter;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(term);
      if (index != null || value != null) {
        sb.append("[");
        if (index != null) {
          sb.append(index);
        }
        if (value != null) {
          if (index != null) {
            sb.append(", ");
          }
          sb.append(value);
        }
        sb.append("]");
      }
      return sb.toString();
    }
  }
  
  public Field field(Term term) {
    for (Field f : columns) {
      if (f.term != null && f.term.equals(term)) return f;
    }
    return null;
  }
  
  public boolean hasTerm(Term term) {
    for (Field f : columns) {
      if (f.term != null && f.term.equals(term)) return true;
    }
    return false;
  }
  
  public boolean hasAnyTerm(Term... terms) {
    for (Term t : terms) {
      if (hasTerm(t)) {
        return true;
      }
    }
    return false;
  }

  public int size() {
    return columns.size();
  }
  
  public boolean isEmpty() {
    return columns.isEmpty();
  }
  
  @Override
  public String toString() {
    return rowType + " ["
        + PathUtils.getFilename(file)
        + " "
        + StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getDelimiter()))
        + " "
        + columns.size()
        + "]";
  }
}
