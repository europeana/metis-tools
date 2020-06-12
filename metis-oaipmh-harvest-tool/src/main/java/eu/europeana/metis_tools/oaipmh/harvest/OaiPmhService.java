package eu.europeana.metis_tools.oaipmh.harvest;

class OaiPmhService {

  private final String oaiPmhEndpoint;
  private final String oaiPmhSetSpec;
  private final String oaiPmhMetadataFormat;

  public OaiPmhService(String oaiPmhEndpoint, String oaiPmhSetSpec, String oaiPmhMetadataFormat) {
    this.oaiPmhEndpoint = oaiPmhEndpoint;
    this.oaiPmhSetSpec = oaiPmhSetSpec;
    this.oaiPmhMetadataFormat = oaiPmhMetadataFormat;
  }

  public String getOaiPmhEndpoint() {
    return oaiPmhEndpoint;
  }

  public String getOaiPmhSetSpec() {
    return oaiPmhSetSpec;
  }

  public String getOaiPmhMetadataFormat() {
    return oaiPmhMetadataFormat;
  }
}
