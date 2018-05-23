package cn.com.leadbank.flume.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.com.leadbank.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.*;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;


/**
 * Function: spooling directory, collect all the historical files and tail the target file;
 * 
 * refer to {@link SpoolDirectorySource}
 * 
 * @author pengzhang
 * @time 2015-12-22
 * 
 */

public class SpoolDirectoryTailFileSource extends AbstractSource implements Configurable, EventDrivenSource{

	private static final Logger logger = LoggerFactory.getLogger(SpoolDirectoryTailFileSource.class);
	
	// Delay used when polling for new files
	private static final int POLL_DELAY_MS = 500;
	
	/* Config options */
	private String spoolDirectory;
	private int batchSize;
	private String trackerDirPath;
	private String deserializerType;
	private Context deserializerContext;
	private String  backupFileDateFormat;
    private String logFilename;
	private String inputCharset;
	private DecodeErrorPolicy decodeErrorPolicy;
	private volatile boolean hasFatalError = false;
	
	private SourceCounter sourceCounter;
	ReliableSpoolDirectoryTailFileEventReader reader;
	private ScheduledExecutorService executor;
	private boolean backoff = true;
	private boolean hitChannelException = false;
	private int maxBackoff;
	
	@Override
	public synchronized void start(){
		logger.info("SpoolDirectoryTailFileSource source starting with directory: {}", spoolDirectory);
		executor = Executors.newSingleThreadScheduledExecutor();
		File directory = new File(spoolDirectory);
		try {
			reader = (new ReliableSpoolDirectoryTailFileEventReader.Builder())
							.spoolDirectory(directory)
							.trackerDirPath(trackerDirPath)
							.deserializerType(deserializerType)
							.deserializerContext(deserializerContext)
							.backupFileDateFormat(backupFileDateFormat)
							.logFilename(logFilename)
							.inputCharset(inputCharset)
							.decodeErrorPolicy(decodeErrorPolicy)
							.build();
		} catch (IOException e) {
			throw new FlumeException("Error instantiating spooling and tail event parser", e);
		}
		
		Runnable runner = new SpoolDirectoryTailFileRunnable(reader, sourceCounter);
		executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);
		
		super.start();
		logger.debug("SpoolDirectoryTailFileSource source started");
		sourceCounter.start();
		
	}
	
	@Override
	public synchronized void stop() {
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.info("Interrupted while awaiting termination", e);
		}
		executor.shutdownNow();
		
		super.stop();
		sourceCounter.stop();
		logger.info("SpoolDirTailFile source {} stopped. Metrics: {}", getName(), sourceCounter);
	}
	@Override
	public String toString() {
		return "Spool Directory Tail File source " + getName() + ": { spoolDir: " + spoolDirectory + " }";
	}
	
	@Override
	public synchronized void configure(Context context) {
		spoolDirectory = context.getString(SPOOL_DIRECTORY,SPOOL_DIRECTORY_DEF);
		Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
		inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
		
		decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY).toUpperCase());
		trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);
		
		deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
		deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));
		
		backupFileDateFormat = context.getString(BACKUP_FILEDATE_FORMAT, DEFAULT_BACKUP_FILEDATE_FORMAT);
		logFilename = context.getString(LOG_FILENAME, DEFAULT_LOG_FILENAME);
		
		maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
		if(sourceCounter == null){
			sourceCounter = new SourceCounter(getName());
		}
		
	}
	private class SpoolDirectoryTailFileRunnable implements Runnable {
		private ReliableSpoolDirectoryTailFileEventReader reader;
		private SourceCounter sourceCounter;
		
		public SpoolDirectoryTailFileRunnable(ReliableSpoolDirectoryTailFileEventReader reader, SourceCounter sourceCounter) {
			this.reader = reader;
			this.sourceCounter = sourceCounter;
		}
		
		@Override
		public void run() {
		  int backoffInterval = 250;
	      try {
	        while (!Thread.interrupted()) {
	          List<Event> events = reader.readEvents(batchSize);
	          if (events.isEmpty()) {
	        	reader.commit();	// Avoid IllegalStateException while tailing file.
	            break;
	          }
	          sourceCounter.addToEventReceivedCount(events.size());
	          sourceCounter.incrementAppendBatchReceivedCount();

	          try {
	            getChannelProcessor().processEventBatch(events);
	            reader.commit();
	          } catch (ChannelException ex) {
	            logger.warn("The channel is full, and cannot write data now. The " +
	              "source will try again after " + String.valueOf(backoffInterval) +
	              " milliseconds");
	            hitChannelException = true;
	            if (backoff) {
	              TimeUnit.MILLISECONDS.sleep(backoffInterval);
	              backoffInterval = backoffInterval << 1;
	              backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
	                                backoffInterval;
	            }
	            continue;
	          }
	          backoffInterval = 250;
	          sourceCounter.addToEventAcceptedCount(events.size());
	          sourceCounter.incrementAppendBatchAcceptedCount();
	        }
	        // logger.info("Spooling Directory Tail File Source runner has shutdown.");
	      } catch (Throwable t) {
	        logger.error("FATAL: " + SpoolDirectoryTailFileSource.this.toString() + ": " +
	            "Uncaught exception in SpoolDirectoryTailSourceSource thread. " +
	            "Restart or reconfigure Flume to continue processing.", t);
	        hasFatalError = true;
	        Throwables.propagate(t);
	      }
		}
	}
}
