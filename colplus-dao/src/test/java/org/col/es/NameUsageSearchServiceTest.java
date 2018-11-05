package org.col.es;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.col.api.TestEntityGenerator;
import org.col.api.model.BareName;
import org.col.api.model.NameUsage;
import org.col.api.model.Page;
import org.col.api.model.ResultPage;
import org.col.api.model.Synonym;
import org.col.api.model.Taxon;
import org.col.api.search.NameSearchRequest;
import org.col.api.search.NameSearchRequest.SortBy;
import org.col.api.search.NameUsageWrapper;
import org.col.es.model.EsNameUsage;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.col.es.EsUtil.*;

import static org.junit.Assert.assertEquals;

public class NameUsageSearchServiceTest extends EsReadTestBase {

  private static final String indexName = "name_usage_test";

  private static RestClient client;

  @BeforeClass
  public static void init() {
    client = esSetupRule.getEsClient();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    EsUtil.deleteIndex(client, indexName);
    client.close();
  }

  @Before
  public void before() {
    EsUtil.deleteIndex(client, indexName);
    EsUtil.createIndex(client, indexName, getEsConfig().nameUsage);
  }

  @Test
  public void testSort1() throws IOException, InvalidQueryException {
    NameUsageTransfer transfer = new NameUsageTransfer(getEsConfig().nameUsage);
    EsNameUsage enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageTaxonWrapper());
    insert(client, indexName, getEsConfig().nameUsage, enu);
    enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageSynonymWrapper());
    insert(client, indexName, getEsConfig().nameUsage, enu);
    enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageBareNameWrapper());
    insert(client, indexName, getEsConfig().nameUsage, enu);
    refreshIndex(client, indexName);
    assertEquals(3, EsUtil.count(client, indexName));
    NameSearchRequest nsr = new NameSearchRequest();
    // Force sorting by index order
    nsr.setSortBy(null);
    NameUsageSearchService svc = new NameUsageSearchService(client, getEsConfig());
    ResultPage<NameUsageWrapper<? extends NameUsage>> result =
        svc.search(indexName, nsr, new Page());
    assertEquals(3, result.getResult().size());
    assertEquals(Taxon.class, result.getResult().get(0).getUsage().getClass());
    assertEquals(Synonym.class, result.getResult().get(1).getUsage().getClass());
    assertEquals(BareName.class, result.getResult().get(2).getUsage().getClass());
  }

  @Test
  public void testSort2() throws IOException, InvalidQueryException {
    NameUsageTransfer transfer = new NameUsageTransfer(getEsConfig().nameUsage);
    EsNameUsage enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageTaxonWrapper());
    // Overwrite to test ordering by scientific name
    enu.setScientificName("B");
    insert(client, indexName, getEsConfig().nameUsage, enu);
    enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageSynonymWrapper());
    enu.setScientificName("C");
    insert(client, indexName, getEsConfig().nameUsage, enu);
    enu = transfer.toEsDocument(TestEntityGenerator.newNameUsageBareNameWrapper());
    enu.setScientificName("A");
    insert(client, indexName, getEsConfig().nameUsage, enu);
    refreshIndex(client, indexName);
    assertEquals(3, EsUtil.count(client, indexName));
    NameSearchRequest nsr = new NameSearchRequest();
    nsr.setSortBy(SortBy.NAME);
    NameUsageSearchService svc = new NameUsageSearchService(client, getEsConfig());
    ResultPage<NameUsageWrapper<? extends NameUsage>> result =
        svc.search(indexName, nsr, new Page());
    assertEquals(3, result.getResult().size());
    assertEquals(BareName.class, result.getResult().get(0).getUsage().getClass());
    assertEquals(Taxon.class, result.getResult().get(1).getUsage().getClass());
    assertEquals(Synonym.class, result.getResult().get(2).getUsage().getClass());
  }

  @Test
  public void testSort3() throws InvalidQueryException, JsonProcessingException {
    NameUsageTransfer transfer = new NameUsageTransfer(getEsConfig().nameUsage);
    NameUsageWrapper<Taxon> nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    // Overwrite to test ordering by key
    nuw.getUsage().getName().setId("3");
    insert(client, indexName, getEsConfig().nameUsage, transfer.toEsDocument(nuw));
    nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    nuw.getUsage().getName().setId("4");
    insert(client, indexName, getEsConfig().nameUsage, transfer.toEsDocument(nuw));
    nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    nuw.getUsage().getName().setId("1");
    insert(client, indexName, getEsConfig().nameUsage, transfer.toEsDocument(nuw));
    nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    nuw.getUsage().getName().setId("5");
    insert(client, indexName, getEsConfig().nameUsage, transfer.toEsDocument(nuw));
    nuw = TestEntityGenerator.newNameUsageTaxonWrapper();
    nuw.getUsage().getName().setId("2");
    insert(client, indexName, getEsConfig().nameUsage, transfer.toEsDocument(nuw));
    refreshIndex(client, indexName);
    NameSearchRequest nsr = new NameSearchRequest();
    nsr.setSortBy(SortBy.KEY);
    NameUsageSearchService svc = new NameUsageSearchService(client, getEsConfig());
    ResultPage<NameUsageWrapper<? extends NameUsage>> result =
        svc.search(indexName, nsr, new Page());
    assertEquals(5, result.getResult().size());
    assertEquals("1", result.getResult().get(0).getUsage().getName().getId());
    assertEquals("2", result.getResult().get(1).getUsage().getName().getId());
    assertEquals("3", result.getResult().get(2).getUsage().getName().getId());
    assertEquals("4", result.getResult().get(3).getUsage().getName().getId());
    assertEquals("5", result.getResult().get(4).getUsage().getName().getId());
  }
}
