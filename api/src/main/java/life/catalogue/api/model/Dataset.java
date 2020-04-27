package life.catalogue.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import life.catalogue.api.constraints.AbsoluteURI;
import life.catalogue.api.constraints.ValidDataset;
import life.catalogue.api.vocab.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Metadata about a dataset or a subset of it if parentKey is given.
 */
@ValidDataset
public class Dataset extends DataEntity<Integer> implements DatasetMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(Dataset.class);

  private Integer key;
  private Integer sourceKey;
  @NotNull
  private DatasetType type;
  @NotNull
  private DatasetOrigin origin;
  private boolean locked = false;
  private boolean privat = false;
  @NotNull
  @NotBlank
  private String title;
  private String alias;
  private UUID gbifKey;
  private UUID gbifPublisherKey;
  private String description;
  private List<String> organisations = Lists.newArrayList();
  private String contact;
  private List<String> authorsAndEditors = Lists.newArrayList();
  private License license;
  private String version;
  private LocalDate released;
  private String citation;
  private String geographicScope;
  @AbsoluteURI
  private URI website;
  private String group;
  @AbsoluteURI
  private URI logo;
  private DataFormat dataFormat;
  @AbsoluteURI
  private URI dataAccess;
  private Frequency importFrequency;
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Integer size;
  @Max(5)
  @Min(1)
  private Integer confidence;
  @Max(100)
  @Min(0)
  private Integer completeness;
  private String notes;
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Set<Integer> contributesTo;
  private LocalDateTime imported;
  private LocalDateTime deleted;
  private Map<DatasetSettings, Object> settings = new HashMap<>();
  private IntSet editors = new IntOpenHashSet();

  @Override
  public Integer getKey() {
    return key;
  }
  
  public DatasetType getType() {
    return type;
  }
  
  public void setType(DatasetType type) {
    this.type = type;
  }
  
  @Override
  public void setKey(Integer key) {
    this.key = key;
  }

  public Integer getSourceKey() {
    return sourceKey;
  }

  public void setSourceKey(Integer sourceKey) {
    this.sourceKey = sourceKey;
  }

  @Override
  public String getTitle() {
    return title;
  }
  
  @Override
  public void setTitle(String title) {
    this.title = title;
  }
  
  public UUID getGbifKey() {
    return gbifKey;
  }
  
  public void setGbifKey(UUID gbifKey) {
    this.gbifKey = gbifKey;
  }
  
  public UUID getGbifPublisherKey() {
    return gbifPublisherKey;
  }
  
  public void setGbifPublisherKey(UUID gbifPublisherKey) {
    this.gbifPublisherKey = gbifPublisherKey;
  }
  
  @Override
  public String getDescription() {
    return description;
  }
  
  @Override
  public void setDescription(String description) {
    this.description = description;
  }
  
  @Override
  public List<String> getAuthorsAndEditors() {
    return authorsAndEditors;
  }
  
  @Override
  public void setAuthorsAndEditors(List<String> authorsAndEditors) {
    this.authorsAndEditors = authorsAndEditors;
  }
  
  @Override
  public List<String> getOrganisations() {
    return organisations;
  }
  
  @Override
  public void setOrganisations(List<String> organisations) {
    this.organisations = organisations;
  }
  
  @Override
  public String getContact() {
    return contact;
  }
  
  @Override
  public void setContact(String contact) {
    this.contact = contact;
  }
  
  @Override
  public License getLicense() {
    return license;
  }
  
  @Override
  public void setLicense(License license) {
    this.license = license;
  }
  
  @Override
  public String getVersion() {
    return version;
  }
  
  @Override
  public void setVersion(String version) {
    this.version = version;
  }
  
  @Override
  public String getGeographicScope() {
    return geographicScope;
  }
  
  @Override
  public void setGeographicScope(String geographicScope) {
    this.geographicScope = geographicScope;
  }
  
  /**
   * Release date of the source data.
   * The date can usually only be taken from metadata explicitly given by the source.
   */
  @Override
  public LocalDate getReleased() {
    return released;
  }
  
  @Override
  public void setReleased(LocalDate released) {
    this.released = released;
  }
  
  @Override
  public String getCitation() {
    return citation;
  }
  
  @Override
  public void setCitation(String citation) {
    this.citation = citation;
  }
  
  @Override
  public URI getWebsite() {
    return website;
  }
  
  @Override
  public void setWebsite(URI website) {
    this.website = website;
  }
  
  public URI getLogo() {
    return logo;
  }
  
  public void setLogo(URI logo) {
    this.logo = logo;
  }
  
  public DataFormat getDataFormat() {
    return dataFormat;
  }
  
  public void setDataFormat(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
  }
  
  public URI getDataAccess() {
    return dataAccess;
  }
  
  public void setDataAccess(URI dataAccess) {
    this.dataAccess = dataAccess;
  }
  
  public DatasetOrigin getOrigin() {
    return origin;
  }
  
  public void setOrigin(DatasetOrigin origin) {
    this.origin = origin;
  }
  
  public boolean isLocked() {
    return locked;
  }

  @JsonProperty("private")
  public boolean isPrivat() {
    return privat;
  }

  public void setPrivat(boolean privat) {
    this.privat = privat;
  }

  public void setLocked(boolean locked) {
    this.locked = locked;
  }
  
  public Frequency getImportFrequency() {
    return importFrequency;
  }
  
  public void setImportFrequency(Frequency importFrequency) {
    this.importFrequency = importFrequency;
  }
  
  public String getNotes() {
    return notes;
  }
  
  public void setNotes(String notes) {
    this.notes = notes;
  }

  public Integer getSize() {
    return size;
  }
  
  /**
   * If the dataset participates in any catalogue assemblies
   * this is indicated here by listing the catalogues datasetKey.
   * <p>
   * Dataset used to build the provisional catalogue will be trusted and insert their names into the names index.
   */
  public Set<Integer> getContributesTo() {
    return contributesTo;
  }
  
  public void setContributesTo(Set<Integer> contributesTo) {
    this.contributesTo = contributesTo;
  }
  
  public void addContributesTo(Integer catalogueKey) {
    if (contributesTo == null) {
      contributesTo = new HashSet<>();
    }
    contributesTo.add(catalogueKey);
  }

  /**
   * Time the data of the dataset was last changed in the Clearinghouse,
   * i.e. time of the last import that changed at least one record.
   */
  public LocalDateTime getImported() {
    return imported;
  }

  public void setImported(LocalDateTime imported) {
    this.imported = imported;
  }


  public LocalDateTime getDeleted() {
    return deleted;
  }
  
  @JsonIgnore
  public boolean hasDeletedDate() {
    return deleted != null;
  }
  
  public void setDeleted(LocalDateTime deleted) {
    this.deleted = deleted;
  }
  
  @Override
  public String getAlias() {
    return alias;
  }
  
  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }
  
  @Override
  public String getGroup() {
    return group;
  }
  
  @Override
  public void setGroup(String group) {
    this.group = group;
  }
  
  @Override
  public Integer getConfidence() {
    return confidence;
  }
  
  @Override
  public void setConfidence(Integer confidence) {
    this.confidence = confidence;
  }
  
  @Override
  public Integer getCompleteness() {
    return completeness;
  }
  
  @Override
  public void setCompleteness(Integer completeness) {
    this.completeness = completeness;
  }

  public Object getSetting(DatasetSettings key) {
    return settings.get(key);
  }

  public String getSettingString(DatasetSettings key) {
    return (String) settings.get(key);
  }

  public Boolean getSettingBool(DatasetSettings key) {
    try {
      return (Boolean) settings.get(key);
    } catch (Exception e) {
      LOG.warn("Failed to convert setting {}={} to boolean", key, settings.get(key), e);
      return null;
    }
  }

  public Integer getSettingInt(DatasetSettings key) {
    try {
      return (Integer) settings.get(key);
    } catch (Exception e) {
      LOG.warn("Failed to convert setting {}={} to integer", key, settings.get(key), e);
      return null;
    }
  }

  public <T extends Enum> T getSettingEnum(DatasetSettings key) {
    try {
      return (T) settings.get(key);
    } catch (Exception e) {
      LOG.warn("Failed to convert setting {}={} to enum", key, settings.get(key), e);
      return null;
    }
  }

  public void putSetting(DatasetSettings key, Object value) {
    if (value == null) {
      settings.remove(key);
    } else if (!key.getType().isInstance(value)){
      throw new IllegalArgumentException("value not of expected type " + key.getType());
    } else {
      settings.put(key, value);
    }
  }

  public boolean hasSetting(DatasetSettings key) {
    return settings.containsKey(key);
  }

  public Map<DatasetSettings, Object> getSettings() {
    return settings;
  }

  public void setSettings(Map<DatasetSettings, Object> settings) {
    this.settings = settings;
  }

  public IntSet getEditors() {
    return editors;
  }

  public void setEditors(IntSet editors) {
    this.editors = editors == null ? new IntOpenHashSet() : editors;
  }

  public void addEditor(int userKey) {
    editors.add(userKey);
  }

  public void removeEditor(int userKey) {
    if (getCreatedBy() != null && userKey == (int) getCreatedBy()) {
      throw new IllegalArgumentException("Original dataset creator cannot be removed from editors");
    }
    editors.remove(userKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    Dataset dataset = (Dataset) o;
    return
        Objects.equals(key, dataset.key) &&
        Objects.equals(sourceKey, dataset.sourceKey) &&
        type == dataset.type &&
        Objects.equals(title, dataset.title) &&
        Objects.equals(alias, dataset.alias) &&
        Objects.equals(gbifKey, dataset.gbifKey) &&
        Objects.equals(gbifPublisherKey, dataset.gbifPublisherKey) &&
        Objects.equals(description, dataset.description) &&
        Objects.equals(organisations, dataset.organisations) &&
        Objects.equals(contact, dataset.contact) &&
        Objects.equals(authorsAndEditors, dataset.authorsAndEditors) &&
        Objects.equals(license, dataset.license) &&
        Objects.equals(version, dataset.version) &&
        Objects.equals(released, dataset.released) &&
        Objects.equals(citation, dataset.citation) &&
        Objects.equals(geographicScope, dataset.geographicScope) &&
        Objects.equals(website, dataset.website) &&
        Objects.equals(group, dataset.group) &&
        Objects.equals(logo, dataset.logo) &&
        dataFormat == dataset.dataFormat &&
        Objects.equals(dataAccess, dataset.dataAccess) &&
        origin == dataset.origin &&
        locked == dataset.locked &&
        privat == dataset.privat &&
        importFrequency == dataset.importFrequency &&
        Objects.equals(confidence, dataset.confidence) &&
        Objects.equals(completeness, dataset.completeness) &&
        Objects.equals(notes, dataset.notes) &&
        Objects.equals(imported, dataset.imported) &&
        Objects.equals(deleted, dataset.deleted) &&
        Objects.equals(settings, dataset.settings) &&
        Objects.equals(editors, dataset.editors);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), key, sourceKey, type, title, alias, gbifKey, gbifPublisherKey, description, organisations,
            contact, authorsAndEditors, license, version, released, citation, geographicScope, website, group, logo,
            dataFormat, dataAccess, origin, locked, privat, importFrequency, confidence, completeness, notes, imported, deleted, settings, editors);
  }
  
  @Override
  public String toString() {
    return "Dataset " + key + ": " + title;
  }
}
