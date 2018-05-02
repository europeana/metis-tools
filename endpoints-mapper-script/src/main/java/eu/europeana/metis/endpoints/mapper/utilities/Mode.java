package eu.europeana.metis.endpoints.mapper.utilities;

/**
 * The Mode that is used to run the script
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public enum Mode {
  CREATE_MAP, REVERSE_MAP, NOT_VALID_MODE;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    return NOT_VALID_MODE;
  }
}
