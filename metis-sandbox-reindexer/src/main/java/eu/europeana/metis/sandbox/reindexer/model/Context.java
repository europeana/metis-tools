package eu.europeana.metis.sandbox.reindexer.model;

import java.util.HashMap;
import java.util.Map;

/**
 * The type Context.
 */
public class Context {
  private final Map<String, Integer> intParams = new HashMap<>();
  private final Map<String, String> stringParams = new HashMap<>();

  /**
   * Gets int params.
   *
   * @return the int params
   */
  public Map<String, Integer> getIntParams() {
    return intParams;
  }

  /**
   * Gets string params.
   *
   * @return the string params
   */
  public Map<String, String> getStringParams() {
    return stringParams;
  }

}
