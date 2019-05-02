package org.col.dao;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.col.api.model.*;
import org.col.api.search.DatasetSearchRequest;
import org.col.api.vocab.Datasets;
import org.col.api.vocab.Origin;
import org.col.api.vocab.TaxonomicStatus;
import org.col.api.vocab.Users;
import org.col.db.mapper.DatasetMapper;
import org.col.db.mapper.DecisionMapper;
import org.col.db.mapper.SectorMapper;
import org.col.db.mapper.TaxonMapper;
import org.gbif.nameparser.api.NameType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecisionRematcher {
  private static final Logger LOG = LoggerFactory.getLogger(DecisionRematcher.class);
  
  private final SqlSession session;
  private final TaxonMapper tm;
  private final DatasetMapper dm;
  private final SectorMapper sm;
  private final DecisionMapper em;
  private final MatchingDao mdao;
  private final int user = Users.DB_INIT;
  
  
  private int sectorTotal = 0;
  private int sectorFailed  = 0;
  private int decisionTotal = 0;
  private int decisionFailed  = 0;
  private int datasets  = 0;
  
  public DecisionRematcher(SqlSession session) {
    this.session = session;
    tm = session.getMapper(TaxonMapper.class);
    dm = session.getMapper(DatasetMapper.class);
    sm = session.getMapper(SectorMapper.class);
    em = session.getMapper(DecisionMapper.class);
    mdao = new MatchingDao(session);
  }
  
  private void clearCounter() {
    sectorTotal = 0;
    sectorFailed  = 0;
    decisionTotal = 0;
    decisionFailed  = 0;
    datasets  = 0;
  }
  
  public void matchAll() {
    clearCounter();
    try {
      execForEachDataset(DecisionRematcher.class.getDeclaredMethod("matchDatasetNoLogs", int.class));
      LOG.info("Rematched {} sectors from all {} datasets, {} failed", sectorTotal, datasets, sectorFailed);
      LOG.info("Rematched {} decisions from all {} datasets, {} failed", decisionTotal, datasets, decisionFailed);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  
  }
  
  public boolean matchSector(Sector s, boolean subject, boolean target) {
    boolean success = true;
    try {
      if (subject) {
        NameUsage t = matchUniquely(s, s.getDatasetKey(), s.getSubject());
        if (t != null) {
          s.getSubject().setId(t.getId());
        } else {
          success = false;
        }
      }
      if (target) {
        NameUsage t = matchUniquely(s, Datasets.DRAFT_COL, s.getTarget());
        if (t != null) {
          s.getTarget().setId(t.getId());
          if (s.getMode() == Sector.Mode.ATTACH) {
            // create single, new child
            Taxon c = newTaxon(Datasets.DRAFT_COL, s.getSubject());
            c.setSectorKey(s.getKey());
            TaxonDao.copyTaxon(session, c, s.getTargetAsDatasetID(), user, Collections.emptySet());
          } else {
            // mark 2 children as coming from this sector...
            for (Taxon c : tm.children(Datasets.DRAFT_COL, t.getId(), new Page(0,2))) {
              c.setSectorKey(s.getKey());
              tm.update(c);
            }
          }
        } else {
          success = false;
        }
      }
      sm.update(s);

    } catch (PersistenceException e) {
      success = false;
      LOG.error("Failed to update rematched sector {}", s, e);
    }
    return success;
  }
  
  private Taxon newTaxon(int datasetKey, SimpleName sn){
    Taxon t = new Taxon();
    t.setDatasetKey(datasetKey);
    t.setStatus(TaxonomicStatus.ACCEPTED);
    
    Name n = new Name();
    t.setName(n);
    n.setDatasetKey(datasetKey);
    n.setScientificName(sn.getName());
    n.setAuthorship(sn.getAuthorship());
    n.setRank(sn.getRank());
    n.setType(NameType.SCIENTIFIC);
    n.setOrigin(Origin.SOURCE);
    return t;
  }
  
  public boolean matchDecision(EditorialDecision ed) {
    NameUsage u = matchUniquely(ed, ed.getDatasetKey(), ed.getSubject());
    boolean success = u != null;
    ed.getSubject().setId(u.getId());
    em.update(ed);
    return success;
  }
  
  private void execForEachDataset(Method m) {
    // just rematch datasets which have sectors
    DatasetSearchRequest req = new DatasetSearchRequest();
    req.setContributesTo(Datasets.DRAFT_COL);
    Page page = new Page(0, 20);
    List<Dataset> resp = null;
    try {
      m.setAccessible(true);
      while(resp == null || resp.size() >= page.getLimit()) {
        resp = dm.search(req, page);
        for (Dataset d : resp) {
            m.invoke(this, (int)d.getKey());
        }
        page.next();
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }
  
  private void matchBrokenSectorTargets(int datasetKey) {
    boolean first = true;
    for (Sector s : session.getMapper(SectorMapper.class).targetBroken(datasetKey)) {
      if (!matchSector(s, false, true)) {
        sectorFailed++;
      }
      sectorTotal++;
      if (first) {
        datasets++;
        first = false;
      }
    }
  }
  
  public void matchBrokenSectorTargets() {
    clearCounter();
    // just rematch datasets which have sectors
    try {
      execForEachDataset(DecisionRematcher.class.getDeclaredMethod("matchBrokenSectorTargets", int.class));
      LOG.info("Rematched {} broken sector targets from {} datasets. {} failed", sectorTotal, datasets, sectorFailed);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }
  
  public void matchDataset(final int datasetKey) {
    matchDatasetNoLogs(datasetKey);
    LOG.info("Rematched {} sectors from dataset {}, {} failed", sectorTotal, datasetKey, sectorFailed);
    LOG.info("Rematched {} decisions from dataset {}, {} failed", decisionTotal, datasetKey, decisionFailed);
  }
  
  private void matchDatasetNoLogs(final int datasetKey) {
    datasets++;
    for (Sector s : sm.listByDataset(datasetKey)) {
      if (!matchSector(s, true, false)){
        sectorFailed++;
      }
      sectorTotal++;
    }
    
    for (EditorialDecision e : em.list(datasetKey, null)) {
      if (!matchDecision(e)){
        decisionFailed++;
      }
      decisionTotal++;
    }
  }
  
  private NameUsage matchUniquely(Decision d, int datasetKey, SimpleName sn){
    List<NameUsage> matches = mdao.matchDataset(sn, datasetKey);
    if (matches.isEmpty()) {
      LOG.warn("{} {} cannot be rematched to dataset {} - lost {}", d.getClass().getSimpleName(), d.getKey(), datasetKey, sn);
    } else if (matches.size() > 1) {
      LOG.warn("{} {} cannot be rematched to dataset {} - multiple names like {}", d.getClass().getSimpleName(), d.getKey(), datasetKey, sn);
    } else {
      return matches.get(0);
    }
    return null;
  }
  
}
