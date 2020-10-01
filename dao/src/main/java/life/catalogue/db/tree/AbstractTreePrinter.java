package life.catalogue.db.tree;

import life.catalogue.api.model.RankedID;
import life.catalogue.api.util.ObjectUtils;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.gbif.nameparser.api.Rank;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Print an entire dataset in a nested way using start/end calls similar to SAX
 */
public abstract class AbstractTreePrinter<T extends RankedID> implements Consumer<T> {
  private int counter = 0;
  protected final int datasetKey;
  protected final Integer sectorKey;
  protected final String startID;
  protected final Set<Rank> ranks;
  protected final Rank lowestRank;
  protected final SqlSessionFactory factory;
  protected SqlSession session;
  private final LinkedList<T> parents = new LinkedList<>();
  protected int level = 0;

  /**
   * @param sectorKey optional sectorKey to restrict printed tree to
   */
  protected AbstractTreePrinter(int datasetKey, Integer sectorKey, String startID, Set<Rank> ranks, SqlSessionFactory factory) {
    this.datasetKey = datasetKey;
    this.startID = startID;
    this.sectorKey = sectorKey;
    this.factory = factory;
    this.ranks = ObjectUtils.coalesce(ranks, Collections.EMPTY_SET);
    if (!this.ranks.isEmpty()) {
      // spot lowest rank
      LinkedList<Rank> rs = new LinkedList<>(this.ranks);
      Collections.sort(rs);
      lowestRank = rs.getLast();
    } else {
      lowestRank = null;
    }
  }

  abstract Cursor<T> iterate();

  abstract String getParentId(T usage);

  /**
   * @return number of written lines, i.e. name usages
   * @throws IOException
   */
  public int print() throws IOException {
    counter = 0;
    try {
      session = factory.openSession(true);
      iterate().forEach(this);

      // send final end signals
      while (!parents.isEmpty()) {
        T p = parents.removeLast();
        if (ranks.isEmpty() || ranks.contains(p.getRank())) {
          end(p);
          level--;
        }
      }

    } finally {
      flush();
      session.close();
    }
    return counter;
  }

  public int getCounter() {
    return counter;
  }
  
  @Override
  public void accept(T u) {
    try {
      // send end signals
      while (!parents.isEmpty() && !parents.peekLast().getId().equals(getParentId(u))) {
        T p = parents.removeLast();
        if (ranks.isEmpty() || ranks.contains(p.getRank())) {
          end(p);
          level--;
        }
      }
      if (ranks.isEmpty() || ranks.contains(u.getRank())) {
        counter++;
        start(u);
        level++;
      }
      parents.add(u);
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void start(T u) throws IOException;

  protected abstract void end(T u) throws IOException;

  protected abstract void flush() throws IOException;

}
