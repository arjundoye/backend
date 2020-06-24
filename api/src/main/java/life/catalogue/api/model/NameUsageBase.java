package life.catalogue.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import life.catalogue.api.vocab.Origin;
import life.catalogue.api.vocab.TaxonomicStatus;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 */
public abstract class NameUsageBase extends DatasetScopedEntity<String> implements NameUsage, SectorEntity {
  
  private Integer sectorKey;
  private Integer verbatimKey;
  @Nonnull
  private Name name;
  @Nonnull
  private TaxonomicStatus status;
  @Nonnull
  private Origin origin;
  private String parentId;
  private String namePhrase;
  private String accordingTo; // read-only
  private String accordingToId;
  private URI link;
  private String remarks;
  /**
   * All bibliographic reference ids for the given name usage
   */
  private List<String> referenceIds = new ArrayList<>();

  public NameUsageBase() {
  }

  public NameUsageBase(NameUsageBase other) {
    super(other);
    this.sectorKey = other.sectorKey;
    this.verbatimKey = other.verbatimKey;
    this.name = other.name;
    this.status = other.status;
    this.origin = other.origin;
    this.parentId = other.parentId;
    this.namePhrase = other.namePhrase;
    this.accordingTo = other.accordingTo;
    this.accordingToId = other.accordingToId;
    this.link = other.link;
    this.remarks = other.remarks;
    this.referenceIds = other.referenceIds;
  }

  @Override
  public Integer getVerbatimKey() {
    return verbatimKey;
  }
  
  @Override
  public void setVerbatimKey(Integer verbatimKey) {
    this.verbatimKey = verbatimKey;
  }

  @Override
  public String getLabel() {
    return getLabel(false);
  }

  @Override
  public String getLabelHtml() {
    return getLabel(true);
  }

  public String getLabel(boolean html) {
    StringBuilder sb;
    if (name != null) {
      sb = name.getLabelBuilder(false);
    } else {
      sb = new StringBuilder();
      sb.append("NULL-NAME");
    }
    if (namePhrase != null) {
      sb.append(" ");
      sb.append(namePhrase);
    }
    if (accordingTo != null) {
      sb.append(" ");
      sb.append(accordingTo);
    }
    return sb.toString();
  }

  @Override
  public Name getName() {
    return name;
  }
  
  public void setName(Name name) {
    this.name = name;
  }
  
  @Override
  public TaxonomicStatus getStatus() {
    return status;
  }
  
  @Override
  public void setStatus(TaxonomicStatus status) {
    this.status = status;
  }
  
  @JsonIgnore
  public void setStatusIfNull(TaxonomicStatus status) {
    if (this.status == null) {
      this.status = Preconditions.checkNotNull(status);
    }
  }

  public String getNamePhrase() {
    return namePhrase;
  }

  public void setNamePhrase(String namePhrase) {
    this.namePhrase = namePhrase;
  }

  @JsonIgnore
  public boolean isProvisional() {
    return status == TaxonomicStatus.PROVISIONALLY_ACCEPTED;
  }
  
  @Override
  public Origin getOrigin() {
    return origin;
  }
  
  @Override
  public void setOrigin(Origin origin) {
    this.origin = origin;
  }
  
  public String getParentId() {
    return parentId;
  }
  
  public void setParentId(String key) {
    this.parentId = key;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public String getAccordingTo() {
    return accordingTo;
  }

  public void setAccordingTo(String accordingTo) {
    this.accordingTo = accordingTo;
  }

  @Override
  public String getAccordingToId() {
    return accordingToId;
  }
  
  public void setAccordingToId(String accordingToId) {
    this.accordingToId = accordingToId;
  }
  
  @Override
  public String getRemarks() {
    return remarks;
  }
  
  @Override
  public void setRemarks(String remarks) {
    this.remarks = remarks;
  }

  public void addRemarks(String remarks) {
    if (!StringUtils.isBlank(remarks)) {
      this.remarks = this.remarks == null ? remarks.trim() : this.remarks + "; " + remarks.trim();
    }
  }

  public Integer getSectorKey() {
    return sectorKey;
  }
  
  public void setSectorKey(Integer sectorKey) {
    this.sectorKey = sectorKey;
  }
  
  public List<String> getReferenceIds() {
    return referenceIds;
  }
  
  public void setReferenceIds(List<String> referenceIds) {
    this.referenceIds = referenceIds;
  }
  
  public SimpleName toSimpleName() {
    return new SimpleName(getId(), name.getScientificName(), name.getAuthorship(), name.getRank());
  }
  
  public URI getLink() {
    return link;
  }
  
  public void setLink(URI link) {
    this.link = link;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof NameUsageBase)) return false;
    if (!super.equals(o)) return false;
    NameUsageBase that = (NameUsageBase) o;
    return Objects.equals(sectorKey, that.sectorKey) &&
      Objects.equals(verbatimKey, that.verbatimKey) &&
      Objects.equals(name, that.name) &&
      status == that.status &&
      origin == that.origin &&
      Objects.equals(parentId, that.parentId) &&
      Objects.equals(namePhrase, that.namePhrase) &&
      Objects.equals(accordingToId, that.accordingToId) &&
      Objects.equals(link, that.link) &&
      Objects.equals(remarks, that.remarks) &&
      Objects.equals(referenceIds, that.referenceIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sectorKey, verbatimKey, name, status, origin, parentId, namePhrase, accordingToId, link, remarks, referenceIds);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name.getLabel(false) + " [" + getId() + "]}";
  }
}
