package life.catalogue.common.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

public class TermSerializer extends Serializer<Term> {
  private final TermFactory TF = TermFactory.instance();

  public TermSerializer() {
    // dont accept null values
    super(false);
  }

  @Override
  public void write(Kryo kryo, Output output, Term term) {
    output.writeString(term.qualifiedName());
  }

  @Override
  public Term read(Kryo kryo, Input input, Class<? extends Term> aClass) {
    return TF.findTerm(input.readString());
  }

  @Override
  public Term copy(Kryo kryo, Term original) {
    return TermFactory.instance().findTerm(original.qualifiedName(), original.isClass());
  }
}