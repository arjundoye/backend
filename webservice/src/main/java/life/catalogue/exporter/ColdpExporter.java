package life.catalogue.exporter;

import life.catalogue.WsServerConfig;
import life.catalogue.api.datapackage.ColdpTerm;
import life.catalogue.api.jackson.ApiModule;
import life.catalogue.api.model.*;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.api.vocab.*;
import life.catalogue.common.io.TermWriter;
import life.catalogue.common.io.UTF8IoUtils;
import life.catalogue.common.text.StringUtils;
import life.catalogue.db.mapper.NameRelationMapper;
import life.catalogue.db.mapper.ProjectSourceMapper;
import life.catalogue.img.ImageService;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.nameparser.api.NomCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class ColdpExporter extends ArchiveExporter {
  private static final Logger LOG = LoggerFactory.getLogger(ColdpExporter.class);
  private static final String METADATA_FILENAME = "metadata.yaml";
  private static final String LOGO_FILENAME = "logo.png";
  private ProjectSourceMapper projectSourceMapper;
  private Writer cslWriter;
  private boolean cslFirst = true;

  public ColdpExporter(ExportRequest req, SqlSessionFactory factory, WsServerConfig cfg, ImageService imageService) {
    super(DataFormat.COLDP, req, factory, cfg, imageService);
  }

  @Override
  protected void init(SqlSession session) throws Exception {
    super.init(session);
    projectSourceMapper = session.getMapper(ProjectSourceMapper.class);
    cslWriter = UTF8IoUtils.writerFromFile(new File(tmpDir, "reference.json"));
    cslWriter.write("[\n");
  }

  @Override
  Term[] define(EntityType entity) {
    switch (entity) {
      case NAME_USAGE:
        return new Term[]{ColdpTerm.NameUsage, ColdpTerm.ID, ColdpTerm.sourceID,
          ColdpTerm.parentID,
          ColdpTerm.basionymID,
          ColdpTerm.status,
          ColdpTerm.scientificName,
          ColdpTerm.authorship,
          ColdpTerm.rank,
          ColdpTerm.uninomial,
          ColdpTerm.genericName,
          ColdpTerm.infragenericEpithet,
          ColdpTerm.specificEpithet,
          ColdpTerm.infraspecificEpithet,
          ColdpTerm.cultivarEpithet,
          ColdpTerm.namePhrase,
          ColdpTerm.nameReferenceID,
          ColdpTerm.publishedInYear,
          ColdpTerm.publishedInPage,
          ColdpTerm.publishedInPageLink,
          ColdpTerm.code,
          ColdpTerm.nameStatus,
          ColdpTerm.accordingToID,
          ColdpTerm.referenceID,
          ColdpTerm.scrutinizer,
          ColdpTerm.scrutinizerID,
          ColdpTerm.scrutinizerDate,
          ColdpTerm.extinct,
          ColdpTerm.temporalRangeStart,
          ColdpTerm.temporalRangeEnd,
          ColdpTerm.environment,
          ColdpTerm.sequenceIndex,
          ColdpTerm.link,
          ColdpTerm.remarks
        };
      case VERNACULAR:
        return new Term[]{ColdpTerm.VernacularName, ColdpTerm.taxonID, ColdpTerm.sourceID,
          ColdpTerm.name,
          ColdpTerm.transliteration,
          ColdpTerm.language,
          ColdpTerm.country,
          ColdpTerm.area,
          ColdpTerm.sex,
          ColdpTerm.referenceID
        };
      case ESTIMATE:
        return new Term[]{ColdpTerm.SpeciesEstimate, ColdpTerm.taxonID, ColdpTerm.sourceID,
          ColdpTerm.estimate,
          ColdpTerm.type,
          ColdpTerm.referenceID,
          ColdpTerm.remarks
        };
      case DISTRIBUTION:
        return new Term[]{ColdpTerm.Distribution, ColdpTerm.taxonID, ColdpTerm.sourceID,
          ColdpTerm.area,
          ColdpTerm.areaID,
          ColdpTerm.gazetteer,
          ColdpTerm.status,
          ColdpTerm.referenceID,
          ColdpTerm.remarks
        };
      case NAME_RELATION:
        return new Term[]{ColdpTerm.NameRelation, ColdpTerm.nameID, ColdpTerm.relatedNameID, ColdpTerm.sourceID,
          ColdpTerm.type,
          ColdpTerm.referenceID,
          ColdpTerm.remarks
        };
      case TYPE_MATERIAL:
        return new Term[]{ColdpTerm.TypeMaterial, ColdpTerm.ID, ColdpTerm.nameID, ColdpTerm.sourceID,
          ColdpTerm.citation,
          ColdpTerm.status,
          ColdpTerm.referenceID,
          ColdpTerm.locality,
          ColdpTerm.country,
          ColdpTerm.latitude,
          ColdpTerm.longitude,
          ColdpTerm.altitude,
          ColdpTerm.host,
          ColdpTerm.date,
          ColdpTerm.collector,
          ColdpTerm.link,
          ColdpTerm.remarks
        };
      case REFERENCE:
        return new Term[]{ColdpTerm.Reference, ColdpTerm.ID, ColdpTerm.sourceID,
          ColdpTerm.citation,
          ColdpTerm.author,
          ColdpTerm.title,
          ColdpTerm.year,
          ColdpTerm.source,
          ColdpTerm.details,
          ColdpTerm.doi,
          ColdpTerm.link,
          ColdpTerm.remarks
        };
    }
    return null;
  }

  @Override
  void write(NameUsageBase u) {
    Name n = u.getName();
    writer.set(ColdpTerm.ID, u.getId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(u.getSectorKey()));
    writer.set(ColdpTerm.parentID, u.getParentId());
    for (NameRelation rel : nameRelMapper.listByType(n, NomRelType.BASIONYM)) {
      writer.set(ColdpTerm.basionymID, rel.getRelatedNameId());
    }
    writer.set(ColdpTerm.status, u.getStatus());
    writer.set(ColdpTerm.scientificName, n.getScientificName());
    writer.set(ColdpTerm.authorship, n.getAuthorship());
    writer.set(ColdpTerm.rank, n.getRank());
    writer.set(ColdpTerm.uninomial, n.getUninomial());
    writer.set(ColdpTerm.genericName, n.getGenus());
    writer.set(ColdpTerm.infragenericEpithet, n.getInfragenericEpithet());
    writer.set(ColdpTerm.specificEpithet, n.getSpecificEpithet());
    writer.set(ColdpTerm.infraspecificEpithet, n.getInfraspecificEpithet());
    writer.set(ColdpTerm.cultivarEpithet, n.getCultivarEpithet());
    writer.set(ColdpTerm.namePhrase, u.getNamePhrase());
    writer.set(ColdpTerm.nameReferenceID, n.getPublishedInId());
    writer.set(ColdpTerm.publishedInYear, n.getPublishedInYear());
    writer.set(ColdpTerm.publishedInPage, n.getPublishedInPage());
    //writer.set(ColdpTerm.publishedInPageLink, null);
    writer.set(ColdpTerm.code, n.getCode());
    writer.set(ColdpTerm.nameStatus, n.getNomStatus());
    writer.set(ColdpTerm.accordingToID, u.getAccordingToId());
    writer.set(ColdpTerm.referenceID, u.getReferenceIds());
    // see taxon specifics below
    writer.set(ColdpTerm.link, u.getLink());
    writer.set(ColdpTerm.remarks, u.getRemarks());

    if (!u.isSynonym()) {
      Taxon t = (Taxon) u;
      writer.set(ColdpTerm.scrutinizer, t.getScrutinizer());
      //writer.set(ColdpTerm.scrutinizerID, null);
      writer.set(ColdpTerm.scrutinizerDate, t.getScrutinizerDate());
      writer.set(ColdpTerm.extinct, t.isExtinct());
      writer.set(ColdpTerm.temporalRangeStart, t.getTemporalRangeStart());
      writer.set(ColdpTerm.temporalRangeEnd, t.getTemporalRangeEnd());
      writer.set(ColdpTerm.environment, t.getEnvironments());
      //writer.set(ColdpTerm.sequenceIndex, null);
    }
  }

  @Override
  void write(String taxonID, VernacularName vn) {
    writer.set(ColdpTerm.taxonID, taxonID);
    writer.set(ColdpTerm.sourceID, sector2datasetKey(vn.getSectorKey()));
    writer.set(ColdpTerm.name, vn.getName());
    writer.set(ColdpTerm.transliteration, vn.getLatin());
    writer.set(ColdpTerm.language, vn.getLanguage());
    writer.set(ColdpTerm.country, vn.getCountry(), Country::getIso2LetterCode);
    writer.set(ColdpTerm.area, vn.getArea());
    writer.set(ColdpTerm.sex, vn.getSex());
  }

  @Override
  void write(Reference r) {
    writer.set(ColdpTerm.ID, r.getId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(r.getSectorKey()));
    writer.set(ColdpTerm.citation, r.getCitation());
    if (r.getCsl() != null) {
      var csl = r.getCsl();
      try {
        if (cslFirst) {
          cslFirst = false;
        } else {
          cslWriter.write(",\n");
        }
        ApiModule.MAPPER.writeValue(cslWriter, csl);
      } catch (IOException e) {
        LOG.warn("Failed to write CSL-JSON for reference {}", r.getId(), e);
      }
      writer.set(ColdpTerm.author, csl.getAuthor());
      writer.set(ColdpTerm.title, csl.getTitle());
      if (csl.getIssued() != null && csl.getIssued().getDateParts() != null) {
        writer.set(ColdpTerm.year, csl.getIssued().getDateParts()[0]);
      }
      writer.set(ColdpTerm.source, ObjectUtils.coalesce(csl.getContainerTitle(), csl.getCollectionTitle()));
      writer.set(ColdpTerm.details, StringUtils.concat(csl.getVolume(), csl.getIssue(), csl.getPage(), r.getPage()));
      writer.set(ColdpTerm.doi, csl.getDOI());
      writer.set(ColdpTerm.link, r.getCsl().getURL());
      writer.set(ColdpTerm.remarks, ObjectUtils.coalesce(r.getRemarks(), csl.getNote()));
    }
  }

  @Override
  void write(NameRelation rel) {
    writer.set(ColdpTerm.nameID, rel.getNameId());
    writer.set(ColdpTerm.relatedNameID, rel.getRelatedNameId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(rel.getSectorKey()));
    writer.set(ColdpTerm.type, rel.getType());
    writer.set(ColdpTerm.referenceID, rel.getReferenceId());
    writer.set(ColdpTerm.remarks, rel.getRemarks());
  }

  @Override
  void write(TypeMaterial tm) {
    writer.set(ColdpTerm.ID, tm.getId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(tm.getSectorKey()));
    writer.set(ColdpTerm.nameID, tm.getNameId());
    writer.set(ColdpTerm.citation, tm.getCitation());
    writer.set(ColdpTerm.status, tm.getStatus());
    writer.set(ColdpTerm.referenceID, tm.getReferenceId());
    writer.set(ColdpTerm.locality, tm.getLocality());
    writer.set(ColdpTerm.country, tm.getCountry(), Country::getIso2LetterCode);
    writer.set(ColdpTerm.latitude, tm.getLatitude());
    writer.set(ColdpTerm.longitude, tm.getLongitude());
    writer.set(ColdpTerm.altitude, tm.getAltitude());
    writer.set(ColdpTerm.host, tm.getHost());
    writer.set(ColdpTerm.altitude, tm.getAltitude());
    writer.set(ColdpTerm.date, tm.getDate());
    writer.set(ColdpTerm.collector, tm.getCollector());
    writer.set(ColdpTerm.link, tm.getLink());
    writer.set(ColdpTerm.remarks, tm.getRemarks());
  }

  @Override
  void write(TaxonConceptRelation rel) {
    writer.set(ColdpTerm.taxonID, rel.getTaxonId());
    writer.set(ColdpTerm.relatedTaxonID, rel.getRelatedTaxonId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(rel.getSectorKey()));
    writer.set(ColdpTerm.type, rel.getType());
    writer.set(ColdpTerm.referenceID, rel.getReferenceId());
    writer.set(ColdpTerm.remarks, rel.getRemarks());
  }

  @Override
  void write(String taxonID, Media m) {
    writer.set(ColdpTerm.taxonID, taxonID);
    writer.set(ColdpTerm.sourceID, sector2datasetKey(m.getSectorKey()));
    writer.set(ColdpTerm.url, m.getUrl());
    writer.set(ColdpTerm.type, m.getType());
    writer.set(ColdpTerm.format, m.getFormat());
    writer.set(ColdpTerm.title, m.getTitle());
    writer.set(ColdpTerm.created, m.getCaptured());
    writer.set(ColdpTerm.creator, m.getCapturedBy());
    writer.set(ColdpTerm.license, m.getLicense());
    writer.set(ColdpTerm.link, m.getLink());
  }

  @Override
  void write(SpeciesInteraction si) {
    writer.set(ColdpTerm.taxonID, si.getTaxonId());
    writer.set(ColdpTerm.relatedTaxonID, si.getRelatedTaxonId());
    writer.set(ColdpTerm.sourceID, sector2datasetKey(si.getSectorKey()));
    writer.set(ColdpTerm.relatedTaxonScientificName, si.getRelatedTaxonScientificName());
    writer.set(ColdpTerm.type, si.getType());
    writer.set(ColdpTerm.referenceID, si.getReferenceId());
    writer.set(ColdpTerm.remarks, si.getRemarks());
  }

  @Override
  void write(String taxonID, Distribution d) {
    writer.set(ColdpTerm.taxonID, taxonID);
    writer.set(ColdpTerm.sourceID, sector2datasetKey(d.getSectorKey()));
    writer.set(ColdpTerm.area, d.getArea());
    //writer.set(ColdpTerm.areaID, d.get());
    writer.set(ColdpTerm.gazetteer, d.getGazetteer());
    writer.set(ColdpTerm.status, d.getStatus());
    writer.set(ColdpTerm.referenceID, d.getReferenceId());
    //writer.set(ColdpTerm.remarks, d.getRemarks());
  }

  @Override
  void write(SpeciesEstimate est) {
    if (est.getTarget() != null) {
      writer.set(ColdpTerm.taxonID, est.getTarget().getId());
      //TODO: writer.set(ColdpTerm.sourceID, null);
      writer.set(ColdpTerm.estimate, est.getEstimate());
      writer.set(ColdpTerm.type, est.getType());
      writer.set(ColdpTerm.referenceID, est.getReferenceId());
      writer.set(ColdpTerm.remarks, est.getNote());
    }
  }

  @Override
  void exportMetadata(Dataset d) throws IOException {
    Set<Integer> sourceKeys = new HashSet<>(sector2datasetKeys.values());
    // for releases and projects also include a source entry
    for (Integer key : sourceKeys) {
      ArchivedDataset src = null;
      if (DatasetOrigin.MANAGED == d.getOrigin()) {
        src = projectSourceMapper.getProjectSource(key, datasetKey);
      } else if (DatasetOrigin.RELEASED == d.getOrigin()) {
        src = projectSourceMapper.getReleaseSource(key, datasetKey);
      }
      if (src == null) {
        LOG.warn("Skip missing dataset {} for archive metadata", key);
        return;
      }
      // TODO: create source entry in dataset, not separate file
      File f = new File(tmpDir, String.format("source/%s.yaml", key));
      DatasetYamlWriter.write(src, f);
    }

    // write to YAML
    DatasetYamlWriter.write(dataset, new File(tmpDir, METADATA_FILENAME));

    // add logo image
    imageService.copyDatasetLogo(datasetKey, new File(tmpDir, LOGO_FILENAME));
  }

  @Override
  protected void bundle() throws IOException {
    cslWriter.write("\n]\n");
    cslWriter.close();
    super.bundle();
  }

}
