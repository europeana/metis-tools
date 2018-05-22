package eu.europeana.metis.endpoints.mapper.utilities;

/**
 * The Mode that is used to run the script
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public enum Mode {
  COPY_WORKFLOW,
  CREATE_OAIPMH_WORKFLOWS,
  CREATE_PREVIEW_WORKFLOWS,
  CREATE_PUBLISH_WORKFLOWS,
  REVERSE_WORKFLOWS,
  NOT_VALID_MODE;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    return NOT_VALID_MODE;
  }
}
