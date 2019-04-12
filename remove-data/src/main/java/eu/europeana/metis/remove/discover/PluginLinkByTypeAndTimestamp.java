package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.util.Date;
import java.util.Objects;

class PluginLinkByTypeAndTimestamp {

  private final PluginType type;
  private final Date timestamp;

  PluginLinkByTypeAndTimestamp(PluginType type, Date timestamp) {
    if (type == null || timestamp == null) {
      throw new IllegalArgumentException();
    }
    this.type = type;
    this.timestamp = new Date(timestamp.getTime());
  }

  public PluginType getType() {
    return type;
  }

  public Date getTimestamp() {
    return new Date(timestamp.getTime());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    final PluginLinkByTypeAndTimestamp that = (PluginLinkByTypeAndTimestamp) other;
    return type == that.type && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, timestamp);
  }
}
