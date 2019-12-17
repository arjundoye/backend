package life.catalogue.db.mapper;

import life.catalogue.api.TestEntityGenerator;
import life.catalogue.api.model.DataEntity;
import life.catalogue.api.model.DatasetScoped;
import life.catalogue.api.model.EditorialDecision;
import life.catalogue.api.search.DecisionSearchRequest;
import life.catalogue.api.vocab.Datasets;
import life.catalogue.api.vocab.Lifezone;
import life.catalogue.api.vocab.TaxonomicStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static life.catalogue.api.TestEntityGenerator.DATASET11;
import static org.junit.Assert.assertEquals;

public class DecisionMapperTest extends CRUDTestBase<Integer, EditorialDecision, DecisionMapper> {
  
  public DecisionMapperTest() {
    super(DecisionMapper.class);
  }
  
  final int catalogeKey = Datasets.DRAFT_COL;
  final int subjectDatasetKey = DATASET11.getKey();
  
  @Test
  public void brokenDecisions() {
    EditorialDecision d1 = createTestEntity(catalogeKey);
    d1.getSubject().setId(TestEntityGenerator.TAXON1.getId());
    mapper().create(d1);

    EditorialDecision d2 = createTestEntity(catalogeKey);
    mapper().create(d2);
    commit();
  
    DecisionSearchRequest req = DecisionSearchRequest.byCatalogue(catalogeKey);
    assertEquals(2, mapper().search(req,null).size());
    
    req.setSubjectDatasetKey(subjectDatasetKey);
    assertEquals(2, mapper().search(req,null).size());
    
    req.setId(TestEntityGenerator.TAXON1.getId());
    assertEquals(1, mapper().search(req,null).size());
  
    req = DecisionSearchRequest.byDataset(catalogeKey, subjectDatasetKey);
    req.setBroken(true);
    assertEquals(1, mapper().search(req,null).size());
  
    req = DecisionSearchRequest.byCatalogue(catalogeKey);
    req.setUserKey(d1.getCreatedBy());
    assertEquals(2, mapper().search(req,null).size());
  
    req.setUserKey(999);
    assertEquals(0, mapper().search(req,null).size());
  }
  
  @Override
  void updateTestObj(EditorialDecision ed) {
    ed.setNote("My next note");
    ed.setName(TestEntityGenerator.newName("updatedID"));
  }
  
  @Override
  EditorialDecision createTestEntity(int dkey) {
    return create(subjectDatasetKey);
  }

  public static EditorialDecision create(int subjectDatasetKey) {
    EditorialDecision d = new EditorialDecision();
    d.setDatasetKey(Datasets.DRAFT_COL);
    d.setSubjectDatasetKey(subjectDatasetKey);
    d.setSubject(TestEntityGenerator.newSimpleName());
    d.setMode(EditorialDecision.Mode.UPDATE);
    d.setName(TestEntityGenerator.newName());
    d.setStatus(TaxonomicStatus.AMBIGUOUS_SYNONYM);
    d.setExtinct(true);
    d.getLifezones().add(Lifezone.MARINE);
    d.getLifezones().add(Lifezone.BRACKISH);
    d.setNote("I cannot remember why I did this.");
    d.setCreatedBy(TestEntityGenerator.USER_EDITOR.getKey());
    d.setModifiedBy(d.getCreatedBy());
    return d;
  }
  
  @Override
  EditorialDecision removeDbCreatedProps(EditorialDecision obj) {
    obj.setCreated(null);
    obj.setModified(null);
    if (obj.getName() != null) {
      // we store the name as JSON and thereby lose its name index id
      obj.getName().setNameIndexId(null);
    }
    return obj;
  }
  
  @Test
  public void process(){
    // processing
    CountHandler handler = new CountHandler();
    mapper().processDataset(catalogeKey).forEach(handler);
    assertEquals(0, handler.counter.size());
  }
  
  public static class CountHandler<T extends DataEntity<Integer> & DatasetScoped> implements Consumer<T> {
    Map<Integer, Integer> counter = new HashMap<>();
  
    @Override
    public void accept(T d) {
      if (counter.containsKey(d.getDatasetKey())) {
        counter.put(d.getDatasetKey(), counter.get(d.getDatasetKey()) + 1);
      } else {
        counter.put(d.getDatasetKey(), 1);
      }
    }
  }
  
}