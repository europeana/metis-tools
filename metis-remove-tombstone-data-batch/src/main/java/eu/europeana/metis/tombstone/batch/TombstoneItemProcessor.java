package eu.europeana.metis.tombstone.batch;

import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.tombstone.config.AppConfiguration;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The type Tombstone item processor.
 */
public class TombstoneItemProcessor implements ItemProcessor<TombstoneEntry, TombstoneEntry> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @Autowired(required = false)
  private IndexerPool indexerPool;

  @Autowired
  private AppConfiguration appConfiguration;

  @Override
  public TombstoneEntry process(final TombstoneEntry entry) throws IndexingException {
    if (appConfiguration.mode().equals("DRY_RUN")) {
      log.info("Dry run mode - skipping actual removal of tombstone for entry ({})", entry.resourceUrl());
      return new TombstoneEntry(entry.europeanaId(), entry.resourceUrl(), "DRY_RUN");
    } else if (appConfiguration.mode().equals("DEFAULT")) {
      boolean result = indexerPool.removeTombstone(entry.europeanaId());
      TombstoneEntry entryProcessed = new TombstoneEntry(entry.europeanaId(), entry.resourceUrl(), result ? "SUCCESS" : "FAILED");
      log.info("Default run mode - processing tombstone ({})", entryProcessed.resourceUrl());
      return entryProcessed;
    }
    return entry;
  }

}
