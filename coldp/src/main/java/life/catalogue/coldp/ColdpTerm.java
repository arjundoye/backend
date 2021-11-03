package life.catalogue.coldp;

import org.gbif.dwc.terms.AlternativeNames;
import org.gbif.dwc.terms.Term;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CoL terms covering all columns needed for the new CoL Data Package submission format:
 * https://github.com/CatalogueOfLife/datapackage-specs
 * <p>
 * To avoid dependency and clashes with DwC no terms are reused.
 */
public enum ColdpTerm implements Term, AlternativeNames {
  Reference(true),
  ID,
  sourceID,
  citation,
  type,
  author,
  editor,
  title,
  containerAuthor,
  containerTitle,
  issued,
  accessed,
  collectionTitle,
  collectionEditor,
  volume,
  issue,
  edition,
  page,
  publisher,
  publisherPlace,
  version,
  isbn,
  issn,
  doi,
  link,
  remarks,

  Name(true),
  //ID,
  //sourceID,
  basionymID(false, "originalNameID"),
  scientificName,
  authorship,
  rank,
  uninomial,
  genus,
  specificEpithet,
  infragenericEpithet,
  infraspecificEpithet,
  cultivarEpithet,
  code,
  referenceID(false, "publishedInID", "namePublishedInID"),
  publishedInYear(false, "namePublishedInYear"),
  publishedInPage(false, "namePublishedInPage"),
  publishedInPageLink(false, "namePublishedInPageLink"),
  status,
  //link,
  //remarks,

  NameRelation(true, "NameRel"),
  nameID,
  relatedNameID,
  //sourceID,
  //type,
  //referenceID,
  //remarks,

  TypeMaterial(true),
  //ID,
  //nameID,
  //sourceID,
  //citation,
  //status,
  locality,
  country,
  latitude,
  longitude,
  altitude,
  host,
  date,
  collector,
  //referenceID,
  //link,
  //remarks,

  Taxon(true),
  // ID,
  //sourceID,
  parentID,
  //nameID,
  namePhrase,
  accordingToID,
  provisional,
  //referenceID,
  scrutinizer,
  scrutinizerID,
  scrutinizerDate,
  extinct,
  temporalRangeStart,
  temporalRangeEnd,
  environment(false, "lifezone"),
  species,
  section,
  subgenus,
  //genus,
  subtribe,
  tribe,
  subfamily,
  family,
  superfamily,
  suborder,
  order,
  subclass,
  class_,
  subphylum,
  phylum,
  kingdom,
  sequenceIndex,
  //link
  //remarks

  Synonym(true),
  //ID
  //sourceID,
  taxonID,
  //nameID
  //appendedPhrase,
  //accordingToID,
  //status
  //referenceID,
  //link
  //remarks

  NameUsage(true),
  nameStatus, // alternative term to Name.status
  nameReferenceID, // alternative term to Name.referenceID
  genericName, // alternative term to Name.genus
  nameRemarks, // alternative term to Name.remarks

  TaxonConceptRelation(true, "TaxonRelation"),
  //taxonID,
  relatedTaxonID,
  //sourceID,
  //type,
  //referenceID,
  //remarks,

  SpeciesInteraction(true),
  //taxonID,
  //relatedTaxonID,
  //sourceID,
  relatedTaxonScientificName,
  //type,
  //referenceID,
  //remarks,

  Treatment(true),
  //taxonID,
  //sourceID,
  document,
  format,

  Distribution(true),
  //taxonID,
  //sourceID,
  areaID,
  area,
  gazetteer,
  //status,
  //referenceID,
  
  Media(true),
  //taxonID,
  //sourceID,
  url,
  //type,
  //format,
  //title,
  created,
  creator,
  license,
  //link,
  
  VernacularName(true),
  //taxonID,
  //sourceID,
  name,
  transliteration,
  language,
  //country,
  sex,
  //referenceID

  SpeciesEstimate(true),
  //taxonID,
  //sourceID,
  estimate,
  //type,
  //referenceID
  //remarks
  ;
  
  private final static Map<String, ColdpTerm> LOOKUP = Arrays.stream(values()).collect(Collectors.toMap(ColdpTerm::normalize, Function.identity()));
  
  /**
   * List of all higher rank terms, ordered by rank and starting with kingdom.
   */
  public static final ColdpTerm[] DENORMALIZED_RANKS = {ColdpTerm.kingdom,
      phylum, subphylum,
      class_, subclass,
      order, suborder,
      superfamily, family, subfamily,
      tribe,subtribe,
      genus, subgenus,
      section,
      species
  };

  public static Map<ColdpTerm, List<ColdpTerm>> RESOURCES = Map.ofEntries(
    Map.entry(Reference, List.of(
      ID,
      sourceID,
      citation,
      type,
      author,
      editor,
      title,
      containerAuthor,
      containerTitle,
      issued,
      accessed,
      collectionTitle,
      collectionEditor,
      volume,
      issue,
      edition,
      page,
      publisher,
      publisherPlace,
      version,
      isbn,
      issn,
      doi,
      link,
      remarks
    )), Map.entry(Name, List.of(
      ID,
      sourceID,
      basionymID,
      scientificName,
      authorship,
      rank,
      uninomial,
      genus,
      infragenericEpithet,
      specificEpithet,
      infraspecificEpithet,
      cultivarEpithet,
      code,
      status,
      referenceID,
      publishedInYear,
      publishedInPage,
      publishedInPageLink,
      link,
      remarks
    )), Map.entry(NameRelation, List.of(
      nameID,
      relatedNameID,
      sourceID,
      type,
      referenceID,
      remarks
    )), Map.entry(TypeMaterial, List.of(
      ID,
      nameID,
      sourceID,
      citation,
      status,
      referenceID,
      locality,
      country,
      latitude,
      longitude,
      altitude,
      host,
      date,
      collector,
      link,
      remarks
    )), Map.entry(Taxon, List.of(
      ID,
      sourceID,
      parentID,
      nameID,
      namePhrase,
      accordingToID,
      provisional,
      scrutinizer,
      scrutinizerID,
      scrutinizerDate,
      extinct,
      temporalRangeStart,
      temporalRangeEnd,
      environment,
      referenceID,
      species,
      section,
      subgenus,
      genus,
      subtribe,
      tribe,
      subfamily,
      family,
      superfamily,
      suborder,
      order,
      subclass,
      class_,
      subphylum,
      phylum,
      kingdom,
      sequenceIndex,
      link,
      remarks
    )), Map.entry(Synonym, List.of(
      ID,
      sourceID,
      taxonID,
      nameID,
      namePhrase,
      accordingToID,
      status,
      referenceID,
      link,
      remarks
    )), Map.entry(NameUsage, List.of(
      ID,
      sourceID,
      parentID,
      basionymID,
      status,
      scientificName,
      authorship,
      rank,
      uninomial,
      genericName,
      infragenericEpithet,
      specificEpithet,
      infraspecificEpithet,
      cultivarEpithet,
      namePhrase,
      nameReferenceID,
      publishedInYear,
      publishedInPage,
      publishedInPageLink,
      code,
      nameStatus,
      accordingToID,
      referenceID,
      scrutinizer,
      scrutinizerID,
      scrutinizerDate,
      extinct,
      temporalRangeStart,
      temporalRangeEnd,
      environment,
      species,
      section,
      subgenus,
      genus,
      subtribe,
      tribe,
      subfamily,
      family,
      superfamily,
      suborder,
      order,
      subclass,
      class_,
      subphylum,
      phylum,
      kingdom,
      sequenceIndex,
      link,
      nameRemarks,
      remarks
    )), Map.entry(SpeciesInteraction, List.of(
      taxonID,
      relatedTaxonID,
      sourceID,
      relatedTaxonScientificName,
      type,
      referenceID,
      remarks
    )), Map.entry(TaxonConceptRelation, List.of(
      taxonID,
      relatedTaxonID,
      sourceID,
      type,
      referenceID,
      remarks
    )), Map.entry(Treatment, List.of(
      taxonID,
      sourceID,
      document,
      format
    )), Map.entry(Distribution, List.of(
      taxonID,
      sourceID,
      areaID,
      area,
      gazetteer,
      status,
      referenceID,
      remarks
    )), Map.entry(Media, List.of(
      taxonID,
      sourceID,
      url,
      type,
      format,
      title,
      created,
      creator,
      license,
      link
    )), Map.entry(VernacularName, List.of(
      taxonID,
      sourceID,
      name,
      transliteration,
      language,
      country,
      area,
      sex,
      referenceID
    )), Map.entry(SpeciesEstimate, List.of(
      taxonID,
      sourceID,
      estimate,
      type,
      referenceID,
      remarks)
    ));

  private static final String PREFIX = "col";
  private static final String NS = "http://catalogueoflife.org/terms/";
  private static final URI NS_URI = URI.create(NS);
  
  private final boolean isClass;
  private final String[] alternatives;
  
  ColdpTerm() {
    this.alternatives = new String[0];
    this.isClass = false;
  }
  
  ColdpTerm(boolean isClass, String... alternatives) {
    this.alternatives = alternatives;
    this.isClass = isClass;
  }
  
  
  @Override
  public String prefix() {
    return PREFIX;
  }
  
  @Override
  public URI namespace() {
    return NS_URI;
  }
  
  @Override
  public String simpleName() {
    if (this == class_) {
      return "class";
    }
    return name();
  }
  
  @Override
  public String toString() {
    return prefixedName();
  }
  
  @Override
  public String[] alternativeNames() {
    return this.alternatives;
  }
  
  @Override
  public boolean isClass() {
    return isClass;
  }
  
  
  private static String normalize(String x, boolean isClass) {
    x = x.replaceAll("[-_ ]+", "").toLowerCase();
    return isClass ? Character.toUpperCase(x.charAt(0)) + x.substring(1) : x;
  }
  
  private static String normalize(ColdpTerm t) {
    return normalize(t.name(), t.isClass);
  }
  
  public static ColdpTerm find(String name, boolean isClass) {
    return LOOKUP.getOrDefault(normalize(name, isClass), null);
  }
}
