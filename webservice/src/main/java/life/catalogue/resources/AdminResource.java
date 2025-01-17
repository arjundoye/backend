package life.catalogue.resources;

import life.catalogue.WsServerConfig;
import life.catalogue.admin.jobs.IndexJob;
import life.catalogue.admin.jobs.ReimportJob;
import life.catalogue.admin.jobs.SectorCountJob;
import life.catalogue.admin.jobs.UsageCountJob;
import life.catalogue.api.model.RequestScope;
import life.catalogue.api.model.User;
import life.catalogue.assembly.AssemblyCoordinator;
import life.catalogue.assembly.AssemblyState;
import life.catalogue.common.collection.IterUtils;
import life.catalogue.common.io.DownloadUtil;
import life.catalogue.common.io.LineReader;
import life.catalogue.concurrent.BackgroundJob;
import life.catalogue.concurrent.JobExecutor;
import life.catalogue.concurrent.JobPriority;
import life.catalogue.dao.DatasetDao;
import life.catalogue.dao.DatasetInfoCache;
import life.catalogue.dw.auth.Roles;
import life.catalogue.es.NameUsageIndexService;
import life.catalogue.gbifsync.GbifSyncJob;
import life.catalogue.gbifsync.GbifSyncManager;
import life.catalogue.img.ImageService;
import life.catalogue.img.LogoUpdateJob;
import life.catalogue.importer.ContinuousImporter;
import life.catalogue.importer.ImportManager;
import life.catalogue.matching.NameIndex;
import life.catalogue.matching.RematchJob;
import life.catalogue.resources.legacy.IdMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.validation.Validator;
import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.dropwizard.auth.Auth;
import io.dropwizard.lifecycle.Managed;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
@RolesAllowed({Roles.ADMIN})
public class AdminResource {
  
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);
  private final SqlSessionFactory factory;
  private final DownloadUtil downloader;
  private final WsServerConfig cfg;
  private final ImageService imgService;
  private final NameUsageIndexService indexService;
  private final NameIndex ni;
  private final ServerSettings settings = new ServerSettings();
  // managed background processes
  private final IdMap idMap;
  private final ImportManager importManager;
  private final ContinuousImporter continuousImporter;
  private final GbifSyncManager gbifSync;
  private final AssemblyCoordinator assembly;
  private final NameIndex namesIndex;
  private final JobExecutor exec;
  private final Validator validator;
  private final DatasetDao ddao;

  public AdminResource(SqlSessionFactory factory, AssemblyCoordinator assembly, DownloadUtil downloader, WsServerConfig cfg, ImageService imgService, NameIndex ni,
                       NameUsageIndexService indexService, ContinuousImporter continuousImporter, ImportManager importManager, DatasetDao ddao, GbifSyncManager gbifSync,
                       NameIndex namesIndex, JobExecutor executor, IdMap idMap, Validator validator) {
    this.factory = factory;
    this.ddao = ddao;
    this.assembly = assembly;
    this.imgService = imgService;
    this.ni = ni;
    this.cfg = cfg;
    this.downloader = downloader;
    this.indexService = indexService;
    this.gbifSync = gbifSync;
    this.continuousImporter = continuousImporter;
    this.importManager = importManager;
    this.namesIndex = namesIndex;
    this.exec = executor;
    this.idMap = idMap;
    this.validator = validator;
  }
  
  public static class ServerSettings {
    public Boolean maintenance = false;
    public Boolean gbifSync;
    public Boolean scheduler;
    public Boolean importer; // import manager & names index
    @Nullable
    @Min(1)
    public Integer importerThreads;
    public boolean idle; // readonly summary of all background processes
  }

  @GET
  @Path("/job")
  public List<BackgroundJob> jobQueue() {
    return exec.getQueue();
  }

  @GET
  @Path("/job/{key}")
  @PermitAll
  public BackgroundJob job(@PathParam("key") UUID key) {
    return exec.getJob(key);
  }

  @DELETE
  @Path("/job/{key}")
  public BackgroundJob cancel(@PathParam("key") UUID key, @Auth User user) {
    return exec.cancel(key, user.getKey());
  }

  @GET
  @Path("/assembly")
  public AssemblyState globalState() {
    return assembly.getState();
  }

  @GET
  @Path("/settings")
  @PermitAll
  public ServerSettings getSettings() {
    settings.scheduler = continuousImporter.hasStarted();
    settings.importer = importManager.hasStarted();
    settings.gbifSync = gbifSync.hasStarted();
    settings.idle = !importManager.hasRunning() // imports
      && exec.isIdle() // background jobs
      && assembly.getState().isIdle(); // syncs
    return settings;
  }
  
  @PUT
  @Path("/settings")
  public synchronized void setSettings(ServerSettings back) throws Exception {
    ServerSettings curr = getSettings();

    if (back.maintenance != null && curr.maintenance != back.maintenance) {
      LOG.info("Set maintenance mode={}", back.maintenance);
      curr.maintenance = back.maintenance;
    }

    if (back.gbifSync != null && curr.gbifSync != back.gbifSync) {
      if (cfg.gbif.syncFrequency < 1) {
        // we started the server with no syncing, give it a reasonable default in hours
        cfg.gbif.syncFrequency = 6;
      }
      LOG.info("Set GBIF Sync to active={}", back.gbifSync);
      startStopManaged(gbifSync, back.gbifSync);
    }
    
    if (back.scheduler != null && curr.scheduler != back.scheduler) {
      if (cfg.importer.continousImportPolling < 1) {
        // we started the server with no polling, give it a reasonable default
        cfg.importer.continousImportPolling = 10;
      }
      LOG.info("Set continuous importer to active={}", back.scheduler);
      startStopManaged(continuousImporter, back.scheduler);
    }

    if (back.importer != null && curr.importer != back.importer) {
      if (back.importerThreads != null && back.importerThreads > 0) {
        cfg.importer.threads = back.importerThreads;
      }
      LOG.info("Set import manager with {} threads & names index to active={}", cfg.importer.threads, back.importer);
      // order is important
      if (back.importer) {
        namesIndex.start();
        importManager.start();
        idMap.start();
      } else {
        importManager.stop();
        namesIndex.stop();
        idMap.stop();
      }
    }
  }
  
  private static void startStopManaged(Managed m, boolean start) throws Exception {
    if (start) {
      m.start();
    } else {
      m.stop();
    }
  }

  @POST
  @Path("/reload-idmap")
  public int reloadIdmap(@Auth User user) throws IOException {
    idMap.reload();
    return idMap.size();
  }

  @POST
  @Path("/logo-update")
  public BackgroundJob updateAllLogos(@Auth User user) {
    return runJob(LogoUpdateJob.updateAllAsync(factory, downloader, cfg.normalizer::scratchFile, imgService, user.getKey()));
  }

  @POST
  @Path("/counter-update")
  public BackgroundJob updateCounter(@Auth User user) {
    return runJob(new UsageCountJob(user, JobPriority.HIGH, factory));
  }

  @POST
  @Path("/gbif-sync")
  @Consumes(MediaType.APPLICATION_JSON)
  public BackgroundJob syncGBIF(List<UUID> keys, @Auth User user) {
    GbifSyncJob job = new GbifSyncJob(cfg.gbif, gbifSync.getClient(), ddao, factory, user.getKey(), keys == null ? null : new HashSet<>(keys));
    return runJob(job);
  }

  @POST
  @Path("/gbif-sync")
  @Consumes(MediaType.TEXT_PLAIN)
  public BackgroundJob syncGBIFText(InputStream keysAsText, @Auth User user) {
    var lr = new LineReader(keysAsText);
    var keys = IterUtils.setOf(lr, UUID::fromString);
    GbifSyncJob job = new GbifSyncJob(cfg.gbif, gbifSync.getClient(), ddao, factory, user.getKey(), keys);
    return runJob(job);
  }

  @POST
  @Path("/reindex")
  public BackgroundJob reindex(@QueryParam("datasetKey") Integer datasetKey, @QueryParam("prio") JobPriority priority, RequestScope req, @Auth User user) {
    if (req == null) {
      req = new RequestScope();
    }
    if (datasetKey != null) {
      req.setDatasetKey(datasetKey);
    }
    if (req.getDatasetKey() == null && !req.getAll()) {
      throw new IllegalArgumentException("Request parameter all or datasetKey must be provided");
    }
    return runJob(new IndexJob(req, user, priority, indexService));
  }

  @POST
  @Path("/rematch")
  public BackgroundJob rematch(@QueryParam("datasetKey") List<Integer> datasetKeys, @Auth User user) {
    if (datasetKeys == null || datasetKeys.isEmpty()) {
      throw new IllegalArgumentException("At least one datasetKey parameter is required");
    } else {
      return runJob(RematchJob.some(user.getKey(),factory,ni, datasetKeys.stream().mapToInt(i->i).toArray()));
    }
  }

  @DELETE
  @Path("/reindex")
  public int createEmptyIndex(@Auth User user) {
    LOG.warn("Drop and recreate empty search index by {}", user);
    return indexService.createEmptyIndex();
  }

  @DELETE
  @Path("/cache")
  public boolean clearCaches(@Auth User user) {
    LOG.info("Clear dataset info cache with {} entries by {}", DatasetInfoCache.CACHE.size(), user);
    DatasetInfoCache.CACHE.clear();
    return true;
  }

  @POST
  @Path("/reimport")
  public BackgroundJob reimport(@Auth User user) {
    return runJob(new ReimportJob(user, factory, importManager, cfg));
  }

  @POST
  @Path("sector-count-update")
  public BackgroundJob updateAllSectorCounts(@QueryParam("datasetKey") Integer datasetKey, @Auth User user) {
    Preconditions.checkArgument(datasetKey != null, "A datasetKey parameter must be given");
    return runJob(new SectorCountJob(user.getKey(), factory, indexService, validator, datasetKey));
  }


  private BackgroundJob runJob(BackgroundJob job){
    exec.submit(job);
    return job;
  }
}
