package eu.europeana.metis.sandbox.reindexer.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 * The type Output.
 */
public class Output {

  /**
   * The constant log.
   */
  private static final Logger LOGGER = LogManager.getLogger(Output.class);
  /**
   * The Client.
   */
  CloudSolrClient client;
  /**
   * The Skip fields.
   */
  List<String> skipFields = new ArrayList<>();
  /**
   * The Context.
   */
  Context context;

  /**
   * Instantiates a new Output.
   *
   * @param context the context
   */
  public Output(Context context) {
    final List<String> zkServers = new ArrayList<>();
    zkServers.add(context.getStringParams().get("targetZkAddress"));
    client = new CloudSolrClient.Builder(zkServers, Optional.empty())
        .build();

    String[] skipFieldsArray = context.getStringParams().get("skipFields").split(",");
    Collections.addAll(skipFields, skipFieldsArray);

    this.context = context;
  }

  /**
   * Write.
   *
   * @param page the page
   * @throws SolrServerException the solr server exception
   * @throws IOException the io exception
   * @throws InterruptedException the interrupted exception
   */
  public void write(SolrDocumentList page) throws SolrServerException, IOException, InterruptedException {
    List<SolrInputDocument> toIndex = new LinkedList<>();

    for (SolrDocument resultDoc : page) {
      SolrInputDocument inputDoc = new SolrInputDocument();

      for (String name : resultDoc.getFieldNames()) {
        if (!skipFields.contains(name)) {
          inputDoc.addField(name, resultDoc.getFieldValue(name));
        }
      }

      toIndex.add(inputDoc);
    }

    int retryCount = 1;
    while (true) {
      try {
        client.add(context.getStringParams().get("targetCollection"), toIndex);
        break;
      } catch (SolrServerException | IOException e) {
        LOGGER.error(e);
        retryCount += 1;
        Thread.sleep(context.getIntParams().get("retryInterval"));
        if (retryCount > context.getIntParams().get("retries")) {
          throw e;
        }
      }
    }

  }
}
