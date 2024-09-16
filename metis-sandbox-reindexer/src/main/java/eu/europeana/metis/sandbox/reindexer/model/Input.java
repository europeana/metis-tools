package eu.europeana.metis.sandbox.reindexer.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

/**
 * The type Input.
 */
public class Input {

  /**
   * The constant log.
   */
  private static final Logger LOGGER = LogManager.getLogger(Input.class);
  /**
   * The Client.
   */
  SolrClient client;
  /**
   * The Cursor mark.
   */
  String cursorMark = CursorMarkParams.CURSOR_MARK_START;
  /**
   * The Q.
   */
  SolrQuery solrQuery;
  /**
   * The Done.
   */
  boolean done = false;
  /**
   * The Context.
   */
  Context context;

  /**
   * Instantiates a new Input.
   *
   * @param context the context
   */
  public Input(Context context) {
    final List<String> zkServers = new ArrayList<>();
    String[] zkAddresses = context.getStringParams().get("sourceZkAddress").split(",");
    Collections.addAll(zkServers, zkAddresses);
    client = new CloudSolrClient.Builder(zkServers, Optional.empty())
        .build();

    solrQuery = new SolrQuery(context.getStringParams().get("query"));

    String uniqueKey = context.getStringParams().get("uniqueKey");
    solrQuery.setSort(SolrQuery.SortClause.asc(uniqueKey));

    solrQuery.setRows(context.getIntParams().get("rows"));

    if (context.getStringParams().get("sourceShards") != null) {
      solrQuery.setParam("shards", context.getStringParams().get("sourceShards"));
    }
    this.context = context;
  }

  /**
   * Gets page.
   *
   * @return the page
   * @throws SolrServerException the solr server exception
   * @throws IOException the io exception
   * @throws InterruptedException the interrupted exception
   */
  public SolrDocumentList getPage() throws SolrServerException, IOException, InterruptedException {
    if (done) {
      // return an empty document list
      return new SolrDocumentList();
    } else {
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      QueryResponse queryResponse;

      int retryCount = 1;
      while (true) {
        try {
          queryResponse = client.query(context.getStringParams().get("sourceCollection"), solrQuery);
          String nextCursorMark = queryResponse.getNextCursorMark();
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
          return queryResponse.getResults();
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

}
