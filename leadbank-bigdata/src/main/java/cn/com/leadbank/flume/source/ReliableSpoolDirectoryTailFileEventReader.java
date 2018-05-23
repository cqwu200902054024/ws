package cn.com.leadbank.flume.source;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static cn.com.leadbank.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.*;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Refer to {@link ReliableSpoolingFileEventReader}
 * 
 * @author pengzheng
 *
 */

public class ReliableSpoolDirectoryTailFileEventReader implements ReliableEventReader{
	private static final Logger logger = LoggerFactory.getLogger(ReliableSpoolDirectoryTailFileEventReader.class);
	static final String metaFileName = ".flumespooltailfile-main.meta";
	
	private final File spoolDirectory;
	private final String deserializerType;
	private final Context deserializerContext;
	private final String backupFileDateFormat;//日志文件日期部分正则表达式
	private final String logFilename;//日志文件名称
	private final File trackerDir;//跟踪文件目录
	private File metaFile;  
	private final Charset inputCharset;
	private final DecodeErrorPolicy decodeErrorPolicy;//遇到无法读取时what to do
	private Optional<FileInfo> currentFile = Optional.absent();
	/**
	 * Create a ReliableSpoolingFileEventReader to watch the given directory.
	 */
	 private ReliableSpoolDirectoryTailFileEventReader(File spoolDirectory, String trackerDirPath,
	      String deserializerType, Context deserializerContext,String backupFileDateFormat,String logFilename, String inputCharset,
	      DecodeErrorPolicy decodeErrorPolicy) throws IOException {
	    // Sanity checks
	    Preconditions.checkNotNull(spoolDirectory);
	    Preconditions.checkNotNull(trackerDirPath);
	    Preconditions.checkNotNull(deserializerType);
	    Preconditions.checkNotNull(deserializerContext);
	    Preconditions.checkNotNull(backupFileDateFormat);
	    Preconditions.checkNotNull(logFilename);
	    Preconditions.checkNotNull(inputCharset);

	    if (logger.isDebugEnabled()) {
	      logger.debug("Initializing {} with directory={}, metaDir={}, " +
	          "deserializer={}",
	          new Object[] { ReliableSpoolDirectoryTailFileEventReader.class.getSimpleName(),
	          spoolDirectory, trackerDirPath, deserializerType });
	    }

	    // Verify directory exists and is readable/writable
	    Preconditions.checkState(spoolDirectory.exists(),
	        "Directory does not exist: " + spoolDirectory.getAbsolutePath());
	    Preconditions.checkState(spoolDirectory.isDirectory(),
	        "Path is not a directory: " + spoolDirectory.getAbsolutePath());
	    
	    this.spoolDirectory = spoolDirectory;
	    this.deserializerType = deserializerType;
	    this.deserializerContext = deserializerContext;
	    this.backupFileDateFormat = backupFileDateFormat;
	    this.logFilename = logFilename;
	    this.inputCharset = Charset.forName(inputCharset);
	    this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
	    this.trackerDir = new File(trackerDirPath);
        // ensure that meta directory exists
        if (!trackerDir.exists()) {
          if (!trackerDir.mkdir()) {
            throw new IOException("Unable to mkdir nonexistent meta directory " +
                    trackerDirPath);
          }
        }
        // ensure that the meta directory is a directory
        if (!trackerDir.isDirectory()) {
          throw new IOException("Specified meta directory is not a directory" +
                  trackerDirPath);
        }
	  }
	
	@Override
	public Event readEvent() throws IOException {
		List<Event> events = readEvents(1);
	    if (!events.isEmpty()) {
	      return events.get(0);
	    } else {
	      return null;
	    }
	}

	@Override
	public List<Event> readEvents(int numEvents) throws IOException {//批量返回
	      checkMetaFile();
	      if (!currentFile.isPresent()) {//FileInfo为空
	          currentFile = getLogFile();
	      }
	      // Return empty list if no new files
	      if (!currentFile.isPresent()) {
	        return Collections.emptyList();
	      }
	    EventDeserializer des = currentFile.get().getDeserializer();
	    List<Event> events = des.readEvents(numEvents);
	    return events;
	}

	/**
	 * 是否创建当天文件的监控文件
	 * isHaveCurrentBackupFile
	 * 
	 * @Description TODO{功能详细描述}
	 * @return boolean
	 * @see
	 */
	private void checkMetaFile() {
        String fileDateString = this.dateStrFormat();
        String metaStr = "." + fileDateString + ".meta";
        metaFile = new File(trackerDir, metaStr);
        File[] files = spoolDirectory.listFiles();
        for (File file:files) {
            if(file.getName().toLowerCase().equals(fileDateString)) {
               if(!metaFile.exists()) {
                   try {
                    metaFile.createNewFile();
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
               }
            }
        }
	}
	
	/**
	 * 获取日志文件
	 * getLogFile TODO {一句话简述功能}
	 * 
	 * @Description TODO{功能详细描述}
	 * @return Optional<FileInfo>
	 * @see
	 */
	private Optional<FileInfo> getLogFile() {
	    File logFile = new File(spoolDirectory, logFilename);
	    return openFile(logFile);
	}
	
	/**
	   * Opens a file for consuming
	   * @param file
	   * @return {@link } for the file to consume or absent option if the
	   * file does not exists or readable.
	   */
	  private Optional<FileInfo> openFile(File file) {    
	    try {
	      // roll the meta file, if needed
	      String path = file.getPath();//目标跟踪文件
	      PositionTracker tracker = DurablePositionTracker.getInstance(metaFile, path);//创建一个文件跟踪器
	      ResettableInputStream in =
	          new ResettableFileInputStream(file, tracker,
	              ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
	              decodeErrorPolicy);//创建一个文件输入流
	      EventDeserializer deserializer = EventDeserializerFactory.getInstance
	          (deserializerType, deserializerContext, in);//将文件反序列化为event

	      return Optional.of(new FileInfo(file, deserializer));//打开文件
	    } catch (FileNotFoundException e) {
	      // File could have been deleted in the interim
	      logger.warn("Could not find file: " + file, e);
	      return Optional.absent();
	    } catch (IOException e) {
	      logger.error("Exception opening file: " + file, e);
	      return Optional.absent();
	    }
	}
	
	/**
	 * 格式化后的日期字符串
	 * dateStrFormat TODO {一句话简述功能}
	 * 
	 * @Description TODO{功能详细描述}
	 * @return String
	 * @see
	 */
	private String dateStrFormat() {
	    SimpleDateFormat dateFormat = new SimpleDateFormat(backupFileDateFormat);
        String fileDateString = dateFormat.format(new Date());
        return fileDateString;
	}
	
   @Override
   public void close() throws IOException {
    if (currentFile.isPresent()) {
      currentFile.get().getDeserializer().close();
      currentFile = Optional.absent();
    }
  }

	/** Commit the last lines which were read. */
   @Override
   public void commit() throws IOException {
	      currentFile.get().getDeserializer().mark();//事件被提交后记录offset(偏移量)
	  }
	/** An immutable class with information about a file being processed. **/
	private static class FileInfo {
		/**
		 * 将文件反序列化为event，默认LineDeserializer，将一行转化为一个事件
		 */
		private final EventDeserializer deserializer;
		public FileInfo(File file, EventDeserializer deserializer){
			this.deserializer = deserializer;
		}
		public EventDeserializer getDeserializer() { return deserializer; }
	}
	
	/**
	 * Special builder class for {@link ReliableSpoolDirectoryTailFileEventReader}
	 */
	public static class Builder {
		private File spoolDirectory;
		private String trackerDirPath = DEFAULT_TRACKER_DIR;
		private String deserializerType = DEFAULT_DESERIALIZER;
		private Context deserializerContext = new Context();
		private String  backupFileDateFormat = BACKUP_FILEDATE_FORMAT;
        private String logFilename = LOG_FILENAME;
		private String inputCharset = DEFAULT_INPUT_CHARSET;
		private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(DEFAULT_DECODE_ERROR_POLICY.toUpperCase());
		
		public Builder spoolDirectory(File directory) {
	      this.spoolDirectory = directory;
	      return this;
	    }
        
	   public Builder trackerDirPath(String trackerDirPath) {
	          this.trackerDirPath = trackerDirPath;
	          return this;
	        } 
		
	    public Builder deserializerType(String deserializerType) {
	      this.deserializerType = deserializerType;
	      return this;
	    }

	    public Builder deserializerContext(Context deserializerContext) {
	      this.deserializerContext = deserializerContext;
	      return this;
	    }
	    
	    public Builder backupFileDateFormat(String backupFileDateFormat) {
	           this.backupFileDateFormat = backupFileDateFormat;
	           return this;
	    }  
	      
	    public Builder logFilename(String logFilename) {
               this.logFilename = logFilename;
               return this;
        }
	    
	    public Builder inputCharset(String inputCharset) {
	      this.inputCharset = inputCharset;
	      return this;
	    }

	    public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
	      this.decodeErrorPolicy = decodeErrorPolicy;
	      return this;
	    }
	    
	    public ReliableSpoolDirectoryTailFileEventReader build() throws IOException {
	        return new ReliableSpoolDirectoryTailFileEventReader(spoolDirectory, 
	             trackerDirPath, deserializerType,
	            deserializerContext, backupFileDateFormat,logFilename, inputCharset, decodeErrorPolicy);
	      }
	}
}