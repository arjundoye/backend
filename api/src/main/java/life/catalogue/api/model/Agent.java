package life.catalogue.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import de.undercouch.citeproc.csl.CSLName;

import de.undercouch.citeproc.csl.CSLNameBuilder;

import life.catalogue.api.vocab.Country;
import life.catalogue.common.util.RegexUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Agent {
  private static final Pattern CAMELCASE = Pattern.compile("\\b(\\p{Lu})(\\p{Ll}+)\\b");

  private static final String GIVEN_NAME = "((?:\\p{Lu}\\p{Ll}+){1,3})";
  private static final String PARTICLES = "(?:al|d[aeiou]?|de[nrmls]?|e|l[ae]s?|[oO]|ter|'?t|v|v[ao]n|zu[rm]?|y)";
  private static final String FAMILY_NAME = "((?:" + PARTICLES + "(?:\\s+|\\s*['´`’]\\s*)){0,2}\\p{Lu}\\p{Ll}\\p{L}*(?:[ -]+\\p{Lu}\\p{Ll}+)?)";
  private static final String INITIALS = "(\\p{Lu}{1,2}(?:[. ]+\\p{Lu}){0,2}\\.?)";
  private static final Pattern FULLNAME = Pattern.compile("^\\s*" + GIVEN_NAME + "\\s+" + FAMILY_NAME + "\\s*$");
  private static final Pattern FULLNAME_REVERSE = Pattern.compile("^\\s*" + FAMILY_NAME + "\\s*,\\s*" + GIVEN_NAME + "\\s*$");
  private static final Pattern SHORTNAME = Pattern.compile("^\\s*" + INITIALS + "\\s+" + FAMILY_NAME + "\\s*$");
  private static final Pattern SHORTNAME_REVERSE = Pattern.compile("^\\s*" + FAMILY_NAME+"(?:\\s+|\\s*,\\s*)" + INITIALS +"\\s*$");
  private static final Pattern BRACKET_SUFFIX = Pattern.compile("^(.+)\\((.+)\\)\\.?\\s*$");
  private static final Pattern EMAIL = Pattern.compile("<?\\s*\\b([A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,})\\s*>?", Pattern.CASE_INSENSITIVE);

  // person properties
  private String orcid;
  private String given;
  private String family;
  // organisation properties
  private String rorid;
  private String organisation;
  private String department;
  private String city;
  private String state;
  private Country country;
  // shared properties
  private String email;
  private String url;
  private String note;

  public static Agent person(String given, String family) {
    return new Agent(null, given, family, null, null, null, null, null, null, null, null, null);
  }

  public static Agent person(String given, String family, String email) {
    return new Agent(null, given, family, null, null, null, null, null, null, email, null, null);
  }

  public static Agent person(String given, String family, String email, String orcid) {
    return new Agent(orcid, given, family, null, null, null, null, null, null, email, null, null);
  }

  public static Agent person(String given, String family, String email, String orcid, String note) {
    return new Agent(orcid, given, family, null, null, null, null, null, null, email, null, note);
  }

  public static Agent contact(String organisation, String email) {
    return new Agent(null, null, null, null, organisation, null, null, null, null, email, null, null);
  }

  public static Agent organisation(String organisation) {
    return new Agent(null, null, null, null, organisation, null, null, null, null, null, null, null);
  }

  public static Agent organisation(String organisation, String department) {
    return new Agent(null, null, null, null, organisation, department, null, null, null, null, null, null);
  }

  public static Agent organisation(String organisation, String department, String city, String state, Country country) {
    return new Agent(null, null, null, null, organisation, department, city, state, country, null, null, null);
  }

  public static Agent organisation(String rorid, String organisation, String department, String city, String state, Country country, String email, String url, String notes) {
    return new Agent(null, null, null, rorid, organisation, department, city, state, country, email, url, notes);
  }

  public static Agent parse(final String raw) {
    if (raw == null) return null;
    Agent p = new Agent();
    parse(p, raw);
    return p;
  }

  static void parse(Agent p, final String originalName) {
    if (!StringUtils.isBlank(originalName) && p != null) {
      String name = originalName;
      Matcher m = BRACKET_SUFFIX.matcher(name);
      if (m.find()) {
        // brackets at the end, often for roles
        name = m.group(1);
        p.setNote(m.group(2));
      }

      // email?
      m = EMAIL.matcher(name);
      if (m.find()) {
        name = m.replaceFirst("");
        p.setEmail(m.group(1));
      }

      // try with 4 distinct & common patterns
      m = FULLNAME.matcher(name);
      if (m.find()) {
        p.setGiven(m.group(1));
        p.setFamily(m.group(2));
      } else {

        m = FULLNAME_REVERSE.matcher(name);
        if (m.find()) {
          p.setFamily(m.group(1));
          p.setGiven(m.group(2));
        } else {

          m = SHORTNAME.matcher(name);
          if (m.find()) {
            p.setGiven(m.group(1));
            p.setFamily(m.group(2));
          } else {

            m = SHORTNAME_REVERSE.matcher(name);
            if (m.find()) {
              RegexUtils.log(m);
              p.setFamily(m.group(1));
              p.setGiven(m.group(2));
            } else {
              // NO LUCK - use family if its a single word, otherwise consider this an organisation name as last resort!
              if (name.contains(" ")) {
                p.setOrganisation(StringUtils.trimToNull(name));
              } else {
                p.setFamily(StringUtils.trimToNull(name));
              }
            }
          }
        }
      }
    }
  }

  public static List<Agent> parse(List<String> names) {
    return names == null ? null : names.stream()
                                       .map(Agent::parse)
                                       .collect(Collectors.toList());
  }

  public static List<Agent> parse(String... names) {
    return names == null ? null : Arrays.stream(names)
                                        .map(Agent::parse)
                                        .collect(Collectors.toList());
  }

  public Agent() {
  }

  public Agent(Agent other) {
    this.orcid = other.orcid;
    this.given = other.given;
    this.family = other.family;
    this.rorid = other.rorid;
    this.organisation = other.organisation;
    this.department = other.department;
    this.city = other.city;
    this.state = other.state;
    this.country = other.country;
    this.email = other.email;
    this.url = other.url;
    this.note = other.note;
  }

  // needed for YAML parser and jackson bindings to support simple strings as agents
  public Agent(String literal) {
    parse(this, literal);
  }

  public Agent(String given, String family) {
    this.given = given;
    this.family = family;
  }

  public Agent(String orcid, String given, String family,
               String rorid, String organisation, String department, String city, String state, Country country,
               String email, String url, String note
  ) {
    this.orcid = orcid;
    this.given = given;
    this.family = family;
    this.rorid = rorid;
    this.organisation = organisation;
    this.department = department;
    this.city = city;
    this.state = state;
    this.country = country;
    this.email = email;
    this.url = url;
    this.note = note;
  }

  @JsonIgnore
  public boolean isPerson(){
    return family != null || given != null;
  }

  @JsonIgnore
  public boolean isOrganisation(){
    return !isPerson() && (organisation != null || department != null);
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public String getName(){
    StringBuilder sb = new StringBuilder();
    if (given == null && family == null) {
      if (department != null) {
        sb.append(department);
      }
      if (organisation != null) {
        if (department != null) {
          sb.append(", ");
        }
        sb.append(organisation);
      }
    } else {
      if (family != null) {
        sb.append(family);
      }
      if (given != null) {
        if (family != null) {
          sb.append(" ");
          sb.append(abbreviate(given));
        } else {
          sb.append(given);
        }
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public String getAddress(){
    if (city == null && state == null && country == null) return null;
    return life.catalogue.common.text.StringUtils.concat(", ", city, state, getCountryTitle());
  }

  public CSLName toCSL() {
    if (isPerson()) {
      return new CSLNameBuilder()
        .given(given)
        .family(family)
        .isInstitution(false)
        .build();
    } else if (isOrganisation()) {
      return new CSLNameBuilder()
        .family(organisation)
        .isInstitution(true)
        .build();
    }
    return null;
  }

  static String abbreviate(String givenName) {
    if (givenName != null) {
      Matcher m = BRACKET_SUFFIX.matcher(givenName);
      if (m.find()) {
        givenName = m.group(1).trim();
      }
      m = CAMELCASE.matcher(givenName);
      if (m.find()) {
        givenName = m.replaceAll("$1.");
      }
    }
    return givenName;
  }

  public String getOrcid() {
    return orcid;
  }

  public String getGiven() {
    return given;
  }

  public void setGiven(String given) {
    this.given = StringUtils.trimToNull(given);
  }
  @Deprecated
  public void setGivenName(String given) {
    setGiven(given);
  }

  public String getFamily() {
    return family;
  }

  public void setFamily(String family) {
    this.family = StringUtils.trimToNull(family);
  }
  @Deprecated
  public void setFamilyName(String family) {
    setFamily(family);
  }

  public String getRorid() {
    return rorid;
  }

  public void setRorid(String rorid) {
    this.rorid = rorid;
  }

  @JsonProperty("organisation")
  public String getOrganisation() {
    return organisation;
  }

  public void setOrganisation(String organisation) {
    this.organisation = organisation;
  }

  public String getDepartment() {
    return department;
  }

  public void setDepartment(String department) {
    this.department = department;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Country getCountry() {
    return country;
  }

  @JsonIgnore
  public String getCountryCode() {
    return country == null ? null : country.getIso2LetterCode();
  }

  @JsonIgnore
  public String getCountryTitle() {
    return country == null ? null : country.getTitle();
  }

  public void setCountry(Country country) {
    this.country = country;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getNote() {
    return note;
  }

  public void setNote(String note) {
    this.note = note;
  }

  public void setOrcid(String orcid) {
    this.orcid = orcid;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return    StringUtils.isBlank(orcid)
           && StringUtils.isBlank(given)
           && StringUtils.isBlank(family)
           && StringUtils.isBlank(rorid)
           && StringUtils.isBlank(organisation)
           && StringUtils.isBlank(department)
           && StringUtils.isBlank(city)
           && StringUtils.isBlank(state)
           && country == null
           && StringUtils.isBlank(email)
           && StringUtils.isBlank(url)
           && StringUtils.isBlank(note);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Agent)) return false;
    Agent agent = (Agent) o;
    return Objects.equals(orcid, agent.orcid)
           && Objects.equals(given, agent.given)
           && Objects.equals(family, agent.family)
           && Objects.equals(rorid, agent.rorid)
           && Objects.equals(organisation, agent.organisation)
           && Objects.equals(department, agent.department)
           && Objects.equals(city, agent.city)
           && Objects.equals(state, agent.state)
           && country == agent.country
           && Objects.equals(email, agent.email)
           && Objects.equals(url, agent.url)
           && Objects.equals(note, agent.note);
  }

  @Override
  public int hashCode() {
    return Objects.hash(orcid, given, family, rorid, organisation, department, city, state, country, email, url, note);
  }

  @Override
  public String toString() {
    return getName();
  }
}
