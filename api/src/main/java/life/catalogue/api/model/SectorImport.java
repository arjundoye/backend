package life.catalogue.api.model;

import java.util.Collection;
import java.util.Objects;
import java.util.Queue;

import com.google.common.collect.EvictingQueue;

public class SectorImport extends ImportMetrics implements SectorEntity {

  private Integer sectorKey;
  private Integer datasetAttempt;

  private final Queue<String> warnings = EvictingQueue.create(25);
  
  public Collection<String> getWarnings() {
    return warnings;
  }
  
  public void setWarnings(Collection<String> warnings) {
    this.warnings.clear();
    this.warnings.addAll(warnings);
  }
  
  public void addWarning(String warning) {
    warnings.add(warning);
  }

  public Integer getSectorKey() {
    return sectorKey;
  }
  
  public void setSectorKey(Integer sectorKey) {
    this.sectorKey = sectorKey;
  }

  public Integer getDatasetAttempt() {
    return datasetAttempt;
  }

  public void setDatasetAttempt(Integer datasetAttempt) {
    this.datasetAttempt = datasetAttempt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SectorImport)) return false;
    if (!super.equals(o)) return false;
    SectorImport that = (SectorImport) o;
    return Objects.equals(sectorKey, that.sectorKey)
           && Objects.equals(datasetAttempt, that.datasetAttempt)
           && Objects.equals(warnings, that.warnings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sectorKey, datasetAttempt, warnings);
  }

  @Override
  public String attempt() {
    return getSectorKey() + "#" + getAttempt();
  }

}
