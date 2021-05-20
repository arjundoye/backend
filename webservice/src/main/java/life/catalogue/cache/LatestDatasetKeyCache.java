package life.catalogue.cache;

import life.catalogue.api.vocab.DatasetOrigin;
import life.catalogue.dao.DatasetInfoCache;
import life.catalogue.db.mapper.DatasetMapper;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.NotFoundException;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Cache that listens to dataset changes and provides the latest dataset keys for project releases
 * and specific release attempts.
 */
public class LatestDatasetKeyCache {
  private SqlSessionFactory factory;
  private final LoadingCache<Integer, Integer> latestRelease = Caffeine.newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(60, TimeUnit.MINUTES)
    .build(k -> lookupLatest(k, false));
  private final LoadingCache<Integer, Integer> latestCandidate = Caffeine.newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(60, TimeUnit.MINUTES)
    .build(k -> lookupLatest(k, true));
  private final LoadingCache<ReleaseAttempt, Integer> releaseAttempt = Caffeine.newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(7, TimeUnit.DAYS)
    .build(this::lookupAttempt);

  public LatestDatasetKeyCache(SqlSessionFactory factory) {
    this.factory = factory;
  }

  public void setSqlSessionFactory(SqlSessionFactory factory) {
    this.factory = factory;
  }

  @Nullable
  public Integer getLatestRelease(@NonNull Integer key) {
    return latestRelease.get(key);
  }

  @Nullable
  public Integer getLatestReleaseCandidate(@NonNull Integer key) {
    return latestCandidate.get(key);
  }

  @Nullable
  public Integer getReleaseAttempt(@NonNull ReleaseAttempt key) {
    return releaseAttempt.get(key);
  }

  public boolean isLatestRelease(int datasetKey) {
    var info = DatasetInfoCache.CACHE.info(datasetKey);
    if (info.origin == DatasetOrigin.RELEASED && info.sourceKey != null) {
      return Objects.equals(getLatestRelease(info.sourceKey), datasetKey);
    }
    return false;
  }

  public static class ReleaseAttempt{
    final int projectKey;
    final int attempt;

    public ReleaseAttempt(int projectKey, int attempt) {
      this.projectKey = projectKey;
      this.attempt = attempt;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReleaseAttempt that = (ReleaseAttempt) o;
      return projectKey == that.projectKey && attempt == that.attempt;
    }

    @Override
    public int hashCode() {
      return Objects.hash(projectKey, attempt);
    }
  }

  /**
   * @param projectKey a dataset key that is known to exist and point to a managed dataset
   * @return dataset key for the latest release of a project or null in case no release exists
   */
  private Integer lookupLatest(int projectKey, boolean candidate) throws NotFoundException {
    try (SqlSession session = factory.openSession()) {
      DatasetMapper dm = session.getMapper(DatasetMapper.class);
      return dm.latestRelease(projectKey, !candidate);
    }
  }

  private Integer lookupAttempt(ReleaseAttempt release) throws NotFoundException {
    try (SqlSession session = factory.openSession()) {
      DatasetMapper dm = session.getMapper(DatasetMapper.class);
      return dm.releaseAttempt(release.projectKey, release.attempt);
    }
  }

  /**
   * Refreshes the latest release(candidate) cache for a given project
   */
  public void refresh(int projectKey) {
    latestRelease.refresh(projectKey);
    latestCandidate.refresh(projectKey);
  }

}
