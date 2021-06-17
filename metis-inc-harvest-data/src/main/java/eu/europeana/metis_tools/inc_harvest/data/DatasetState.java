package eu.europeana.metis_tools.inc_harvest.data;

import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;

public class DatasetState {

  private ExecutablePlugin harvestPlugin;
  private ExecutablePlugin previewPlugin;
  private ExecutablePlugin publishPlugin;
  private ExecutablePlugin previewHarvestPlugin;
  private ExecutablePlugin publishedHarvestPlugin;

  public ExecutablePlugin getHarvestPlugin() {
    return harvestPlugin;
  }

  public void setHarvestPlugin(ExecutablePlugin harvestPlugin) {
    this.harvestPlugin = harvestPlugin;
  }

  public ExecutablePlugin getPreviewPlugin() {
    return previewPlugin;
  }

  public void setPreviewPlugin(ExecutablePlugin previewPlugin) {
    this.previewPlugin = previewPlugin;
  }

  public ExecutablePlugin getPublishPlugin() {
    return publishPlugin;
  }

  public void setPublishPlugin(ExecutablePlugin publishPlugin) {
    this.publishPlugin = publishPlugin;
  }

  public ExecutablePlugin getPreviewHarvestPlugin() {
    return previewHarvestPlugin;
  }

  public void setPreviewHarvestPlugin(
          ExecutablePlugin previewHarvestPlugin) {
    this.previewHarvestPlugin = previewHarvestPlugin;
  }

  public ExecutablePlugin getPublishedHarvestPlugin() {
    return publishedHarvestPlugin;
  }

  public void setPublishedHarvestPlugin(
          ExecutablePlugin publishedHarvestPlugin) {
    this.publishedHarvestPlugin = publishedHarvestPlugin;
  }
}
