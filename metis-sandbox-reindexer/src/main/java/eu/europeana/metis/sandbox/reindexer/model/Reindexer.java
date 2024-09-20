package eu.europeana.metis.sandbox.reindexer.model;

import eu.europeana.metis.sandbox.reindexer.exceptions.ReindexerException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

/**
 * The type Reindexer.
 */
public class Reindexer {

  /**
   * The constant LOGGER.
   */
  private static final Logger LOGGER = LogManager.getLogger(Reindexer.class);
  /**
   * The Context.
   */
  Context context;
  private final AtomicBoolean isFinished = new AtomicBoolean(false);
  private final BlockingQueue<SolrDocumentList> queue;

  /**
   * Instantiates a new Reindexer.
   *
   * @param context the context
   */
  public Reindexer(Context context) {
    this.context = context;
    this.queue = new LinkedBlockingQueue<>(context.getIntParams().get("queueSize"));
  }

  /**
   * Run.
   */
  public void run() {
    Thread readerThread = new Thread(new Reader(queue, context, isFinished));
    readerThread.start();
    ArrayList<Thread> writeThreads = new ArrayList<>();
    for (int i = 0; i < context.getIntParams().get("numWriteThreads"); i++) {
      Thread writerThread = new Thread(new Writer(queue, context, isFinished));
      writerThread.start();
      writeThreads.add(writerThread);
    }

    try {
      readerThread.join();
      LOGGER.info("Waiting write threads to empty the queue...");
      for (Thread writeThread : writeThreads) {
        writeThread.join();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * The type Reader.
   */
  static class Reader implements Runnable {

    /**
     * The Context.
     */
    Context context;
    /**
     * The Input.
     */
    Input input;
    private final BlockingQueue<SolrDocumentList> queue;
    private final AtomicBoolean isFinished;

    /**
     * Instantiates a new Reader.
     *
     * @param queue the queue
     * @param context the context
     * @param isFinished the is finished
     */
    Reader(BlockingQueue<SolrDocumentList> queue, Context context, AtomicBoolean isFinished) {
      this.queue = queue;
      this.context = context;
      this.input = new Input(context);
      this.isFinished = isFinished;
    }

    @Override
    public void run() {
      try {
        SolrDocumentList page = input.getPage();
        long totalDocs = page.getNumFound();
        long currentPage = 1;
        int rowsPerPage = context.getIntParams().get("rows");
        long totalPages = (totalDocs + rowsPerPage - 1) / rowsPerPage;

        while ((page != null) && !page.isEmpty()) {
          // Read from Solr here and add it to the queue
          LOGGER.info("Reading page {} of {}", currentPage, totalPages);

          queue.put(page);

          page = input.getPage();
          currentPage += 1;
        }

        isFinished.set(true);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (SolrServerException | IOException e) {
        throw new eu.europeana.metis.sandbox.reindexer.exceptions.ReindexerException("Reindexing read", e);
      }
    }
  }

  /**
   * The type Writer.
   */
  static class Writer implements Runnable {

    /**
     * The Context.
     */
    Context context;
    /**
     * The Output.
     */
    Output output;
    private final BlockingQueue<SolrDocumentList> queue;
    private final AtomicBoolean isFinished;

    /**
     * Instantiates a new Writer.
     *
     * @param queue the queue
     * @param context the context
     * @param isFinished the is finished
     */
    Writer(BlockingQueue<SolrDocumentList> queue, Context context, AtomicBoolean isFinished) {
      this.queue = queue;
      this.context = context;
      this.output = new Output(context);
      this.isFinished = isFinished;
    }

    @Override
    public void run() {
      try {
        while (true) {
          SolrDocumentList page = queue.poll(); // This will not block, returns null if queue is empty
          if (page != null) {
            LOGGER.info("Write page {}", queue.size());
            output.write(page);
          } else if (isFinished.get()) {
            break; // exit when done
          } else {
            Thread.sleep(50); // If no data, sleep for a while
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (SolrServerException | IOException e) {
        throw new ReindexerException("Reindexing write", e);
      }
    }
  }
}
