package life.catalogue.resources;

import life.catalogue.WsServerConfig;
import life.catalogue.api.exception.NotFoundException;
import life.catalogue.api.model.*;
import life.catalogue.api.search.NameUsageSearchParameter;
import life.catalogue.api.search.NameUsageSearchRequest;
import life.catalogue.api.vocab.TaxonomicStatus;
import life.catalogue.dao.DatasetImportDao;
import life.catalogue.db.mapper.NameUsageMapper;
import life.catalogue.db.tree.JsonTreePrinter;
import life.catalogue.db.tree.TaxonCounter;
import life.catalogue.db.tree.TextTreePrinter;
import life.catalogue.dw.jersey.MoreMediaTypes;
import life.catalogue.dw.jersey.filter.VaryAccept;
import life.catalogue.es.NameUsageSearchService;
import life.catalogue.exporter.ExportManager;

import org.gbif.nameparser.api.Rank;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.io.IOUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;

import io.dropwizard.auth.Auth;

/**
 * Stream dataset exports to the user.
 * If existing it uses preprepared files from the filesystem.
 * For yet non existing files we should generate and store them for later reuse.
 * If no format is given the original source is returned.
 *
 * Managed datasets can change data continously and we will need to:
 *  a) never store any version and dynamically recreate them each time
 *  b) keep a "dirty" flag that indicates the currently stored archive is not valid anymore because data has changed.
 *     Any edit would have to raise the dirty flag which therefore must be kept in memory and only persisted if it has changed.
 *     Creating an export would remove the flag - we will need a flag for each supported output format.
 *
 * Formats currently supported for the entire dataset and which are archived for reuse:
 *  - ColDP
 *  - ColDP simple (single TSV file)
 *  - DwCA
 *  - DwC simple (single TSV file)
 *  - TextTree
 *
 *  Single file formats for dynamic exports using some filter (e.g. rank, taxonID, etc)
 *  - ColDP simple (single TSV file)
 *  - DwC simple (single TSV file)
 *  - TextTree
 */
@Path("/dataset/{key}/export")
@Produces(MediaType.APPLICATION_JSON)
public class DatasetExportResource {
  private final DatasetImportDao diDao;
  private final SqlSessionFactory factory;
  private final NameUsageSearchService searchService;
  private final ExportManager exportManager;
  private final WsServerConfig cfg;
  private static final Object[][] EXPORT_HEADERS = new Object[1][];
  static {
    EXPORT_HEADERS[0] = new Object[]{"ID", "parentID", "status", "rank", "scientificName", "authorship", "label"};
  }

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(DatasetExportResource.class);

  public DatasetExportResource(SqlSessionFactory factory, NameUsageSearchService searchService, ExportManager exportManager, DatasetImportDao diDao, WsServerConfig cfg) {
    this.factory = factory;
    this.searchService = searchService;
    this.exportManager = exportManager;
    this.diDao = diDao;
    this.cfg = cfg;
  }

  @POST
  public UUID export(@PathParam("key") int key, @Valid ExportRequest req, @Auth User user) {
    if (req == null) req = new ExportRequest();
    req.setDatasetKey(key);
    req.setForce(false); // we don't allow to force exports from the public API
    return exportManager.submit(req, user.getKey());
  }

  @GET
  @VaryAccept
  // there are many unofficial mime types around for zip
  @Produces({
    MediaType.APPLICATION_OCTET_STREAM,
    MoreMediaTypes.APP_ZIP, MoreMediaTypes.APP_ZIP_ALT1, MoreMediaTypes.APP_ZIP_ALT2, MoreMediaTypes.APP_ZIP_ALT3
  })
  public Response original(@PathParam("key") int key) {
    File source = cfg.normalizer.source(key);
    if (source.exists()) {
      StreamingOutput stream = os -> {
        InputStream in = new FileInputStream(source);
        IOUtils.copy(in, os);
        os.flush();
      };

      return Response.ok(stream)
        .type(MoreMediaTypes.APP_ZIP)
        .build();
    }
    throw new NotFoundException(key, "original archive for dataset " + key + " not found");
  }

  @GET
  @VaryAccept
  @Path("{id}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response textTree(@PathParam("key") int key,
                           @PathParam("id") String taxonID,
                           @QueryParam("rank") Set<Rank> ranks,
                           @Context SqlSession session) {
    StreamingOutput stream = os -> {
      Writer writer = new BufferedWriter(new OutputStreamWriter(os));
      TextTreePrinter printer = TextTreePrinter.dataset(key, taxonID, ranks, factory, writer);
      printer.print();
      if (printer.getCounter().isEmpty()) {
        writer.write("--NONE--");
      }
      writer.flush();
    };
    return Response.ok(stream).build();
  }

  @GET
  @VaryAccept
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{id}")
  public Object simpleName(@PathParam("key") int key,
                           @PathParam("id") String taxonID,
                           @QueryParam("rank") Set<Rank> ranks,
                           @QueryParam("synonyms") boolean includeSynonyms,
                           @QueryParam("nested") boolean nested,
                           @QueryParam("countRank") Rank countRank,
                           @Context SqlSession session) {
    final TaxonCounter counter = countRank == null ? null : new TaxonCounter() {
      final Page page = new Page(0,0);
      @Override
      public int count(DSID<String> taxonID, Rank countRank) {
        NameUsageSearchRequest req = new NameUsageSearchRequest();
        req.addFilter(NameUsageSearchParameter.DATASET_KEY, key);
        req.addFilter(NameUsageSearchParameter.TAXON_ID, taxonID);
        req.addFilter(NameUsageSearchParameter.RANK, countRank);
        req.addFilter(NameUsageSearchParameter.STATUS, TaxonomicStatus.ACCEPTED, TaxonomicStatus.PROVISIONALLY_ACCEPTED);
        var resp = searchService.search(req, page);
        return resp.getTotal();
      }
    };

    if (nested) {
      StreamingOutput stream;
      stream = os -> {
        Writer writer = new BufferedWriter(new OutputStreamWriter(os));
        JsonTreePrinter.dataset(key, taxonID, ranks, countRank, counter, factory, writer).print();
        writer.flush();
      };
      return Response.ok(stream).build();

    } else {
      // spot lowest rank
      Rank lowestRank = null;
      if (!ranks.isEmpty()) {
        LinkedList<Rank> rs = new LinkedList<>(ranks);
        Collections.sort(rs);
        lowestRank = rs.getLast();
      }
      var cursor = session.getMapper(NameUsageMapper.class).processTreeSimple(key, null, taxonID, null, lowestRank, includeSynonyms);
      // add counts?
      if (countRank != null) {
        final DSID<String> id = DSID.of(key, taxonID);
        return StreamSupport.stream(cursor.spliterator(), false)
                            .map(sn -> new SNC(sn, counter.count(id.id(sn.getId()), countRank)));
      }
      return cursor;
    }
  }

  static class SNC extends SimpleName {
    public final int count;

    public SNC(SimpleName sn, int count) {
      super(sn);
      this.count = count;
    }
  }


  @GET
  @VaryAccept
  @Produces({MoreMediaTypes.TEXT_CSV, MoreMediaTypes.TEXT_TSV})
  @Path("{id}")
  public Stream<Object[]> exportCsv(@PathParam("key") int datasetKey,
                                    @PathParam("id") String taxonID,
                                    @QueryParam("rank") Rank rank,
                                    @QueryParam("synonyms") boolean synonyms,
                                    @Context SqlSession session) {
    NameUsageMapper num = session.getMapper(NameUsageMapper.class);

    return Stream.concat(
      Stream.of(EXPORT_HEADERS),
      Streams.stream(num.processTreeSimple(datasetKey, null, taxonID, null, rank, synonyms)).map(this::map)
    );
  }

  private Object[] map(SimpleName sn){
    return new Object[]{
      sn.getId(),
      sn.getParent(),
      sn.getStatus(),
      sn.getRank(),
      sn.getName(),
      sn.getAuthorship(),
      sn.getPhrase(),
      sn.getLabel()
    };
  }

}
