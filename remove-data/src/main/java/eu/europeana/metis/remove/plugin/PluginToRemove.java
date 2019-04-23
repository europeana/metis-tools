package eu.europeana.metis.remove.plugin;

import eu.europeana.metis.core.workflow.plugins.PluginType;

class PluginToRemove {

  private final String executionId;
  private final String pluginId;
  private final PluginType pluginType;

  PluginToRemove(String executionId, String pluginId, PluginType pluginType) {
    this.executionId = executionId;
    this.pluginId = pluginId;
    this.pluginType = pluginType;
  }

  public String getExecutionId() {
    return executionId;
  }

  public String getPluginId() {
    return pluginId;
  }

  public PluginType getPluginType() {
    return pluginType;
  }
}
