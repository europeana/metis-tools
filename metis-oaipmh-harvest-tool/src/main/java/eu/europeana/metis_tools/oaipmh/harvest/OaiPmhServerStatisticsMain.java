package eu.europeana.metis_tools.oaipmh.harvest;

import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OaiPmhServerStatisticsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OaiPmhServerStatisticsMain.class);

  public static void main(String[] args) throws HarvesterException {

    // DEFINE THE INPUT HERE
    final OaiPmhService service = new OaiPmhService(
        "https://flow-api.repox.io/oai-pmh/", "d138", "edm");

    // Define outcome variables
    final Set<String> oaiIds = new HashSet<>();
    final AtomicInteger uniqueRecordsFound = new AtomicInteger();
    final AtomicInteger deletedRecordsFound = new AtomicInteger();
    final AtomicInteger duplicateRecordsFound = new AtomicInteger();
    final AtomicInteger totalRecordsFound = new AtomicInteger();

    // Pass by all records
    final OaiHarvester harvester = HarvesterFactory.createOaiHarvester();
    harvester.harvestRecordHeaders(new OaiHarvest(service.getOaiPmhEndpoint(),
        service.getOaiPmhMetadataFormat(), service.getOaiPmhSetSpec())).forEach(header -> {
      if (header.isDeleted()) {
        deletedRecordsFound.incrementAndGet();
      } else if (oaiIds.add(header.getOaiIdentifier())) {
        uniqueRecordsFound.incrementAndGet();
      } else {
        duplicateRecordsFound.incrementAndGet();
      }
      final int total = totalRecordsFound.incrementAndGet();
      if (total % 1000 == 0) {
        LOGGER.info("Processed {} records.", total);
      }
      return IterationResult.CONTINUE;
    });

    // Report statistics
    LOGGER.info("Statistics: ");
    LOGGER.info("  Unique record IDs found: {}", uniqueRecordsFound.get());
    LOGGER.info("  Deleted records found: {}", deletedRecordsFound.get());
    LOGGER.info("  Duplicate record IDs found: {}", duplicateRecordsFound.get());
    LOGGER.info("TOTAL: {}", totalRecordsFound.get());
  }
}
