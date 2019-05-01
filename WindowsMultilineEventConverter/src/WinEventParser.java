import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WinEventParser {
	
	private static final Logger logger = Logger.getLogger(WinEventParser.class.getName());

	public static void main(String[] args) {
        
		long startTime = System.currentTimeMillis();		

		String SourceFolder = "";
		String DestinationFolder = "";
		String InputFilePrefix = "";
		String OutputFilePrefix = "";
		
		final String TAB_SEPARATOR=" +";
		final String SPACE_SEPARATOR = " ";
		
		try {
			
			File file = new File("WinEventParser.properties");
			
			if (file.exists() && !file.isDirectory()) 
			{
				FileInputStream fileInput = new FileInputStream(file);
				Properties properties = new Properties();
				properties.load(fileInput);
				fileInput.close();

				SourceFolder = properties.getProperty("SOURCE_FOLDER").trim();
				DestinationFolder = properties.getProperty("OUTPUT_FOLDER").trim();
				InputFilePrefix = properties.getProperty("INPUT_FILE_PREFIX").trim();
				OutputFilePrefix = properties.getProperty("OUTPUT_FILE_PREFIX").trim();
				updateLogger(properties.getProperty("LOG_LEVEL").trim());
					
				logger.log(Level.INFO,"Properties File Loaded......");
				logger.log(Level.CONFIG,"SOURCE FOLDER : " + SourceFolder);
				logger.log(Level.CONFIG,"DESTINATION FOLDER: " + DestinationFolder);
				logger.log(Level.CONFIG,"INPUT FILE PREFIX : " + InputFilePrefix);
				logger.log(Level.CONFIG,"OUTPUT FILE PREFIX : " + OutputFilePrefix);
				logger.log(Level.CONFIG,"LOG LEVEL : " + properties.getProperty("LOG_LEVEL").trim());

			} else {
				logger.log(Level.SEVERE,"Properties File Not Found......");
				logger.log(Level.SEVERE,"Quitting Execution....");
				System.exit(0);
			}
						
			File[] files = new File(SourceFolder).listFiles();
			
			StringBuilder sb = new StringBuilder();
			
			FileReader fileReader = null;
			BufferedReader bufferedReader = null;		
			
			Arrays.sort(files);
			for (File rawfile : files) 
			{
				if (rawfile.getName().startsWith(InputFilePrefix) && !rawfile.isDirectory() && rawfile.exists()) 
				{
					File input = new File(rawfile.getAbsolutePath());

					logger.log(Level.INFO,"Started Parsing of File: " + input.getName());
					fileReader = new FileReader(input.getAbsolutePath());
					bufferedReader = new BufferedReader(fileReader);
					String line = "";
					
					List<String> events = new ArrayList<String>();
					int eventCounter = 0;
					boolean isEventComplete = false ;
					Pattern pattern = Pattern.compile(".*\\s+(\\d+\\/\\d+\\/\\d+\\s+\\d+\\:\\d+:\\d+\\s+\\S+)");
					Matcher matcher;
					
					while ((line = bufferedReader.readLine()) != null) 
					{
						matcher = pattern.matcher(line);
						if (matcher.find())
						{
								line = matcher.group(1).trim();
								if(sb.length()>0)
									isEventComplete=true;
						}
												
						if(isEventComplete && sb.length() > 0 )
						{
							matcher = pattern.matcher(sb);
							if (matcher.find()) {
								events.add(sb.toString());
								eventCounter++;
							}
							
							sb.setLength(0);
							isEventComplete = false;
							if(eventCounter==1000)
							{
								Print(DestinationFolder + OutputFilePrefix + input.getName() , events); 
								eventCounter=0;
								events.clear();
							}
						} else
						{
							isEventComplete = false;
						}
						
						if(line.trim().length()>0)
						sb.append(line.trim().replaceAll(TAB_SEPARATOR,SPACE_SEPARATOR) + SPACE_SEPARATOR);					
							
					}
					if(!isEventComplete && sb.length() > 0 )
						events.add(sb.toString());
					fileReader.close();
					bufferedReader.close();		
					
					Print(DestinationFolder + OutputFilePrefix + input.getName() , events);
				}
				else {
					logger.log(Level.CONFIG,"Ignoring File :" + rawfile.getName());
				}

			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;

		logger.log(Level.INFO,"Time of Execution : " + TimeUnit.MILLISECONDS.toMinutes(elapsedTime) + " Min " + elapsedTime
				+ " milliseconds");

	}

	private static void Print(String filename,List<String> message)
	  {
		  PrintWriter writer = null;
		  try {
			  
			  File outputFile = new File(filename);
			  if(!outputFile.exists()) outputFile.createNewFile();
			  writer =  new PrintWriter(new FileOutputStream(outputFile, true));
			  for( String evt : message)
			  {
				  writer.println(evt);
			  }
		  } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer!=null) writer.close();
		}
	  }
	
	private static void updateLogger(String level) {
		
		System.setProperty("java.util.logging.SimpleFormatter.format","%1$tF %1$tT - %4$s - %2$s - %5$s%6$s%n");
		
		ConsoleHandler ch = new ConsoleHandler();
		
		Level LEVEL =  null;
								
		switch (level.toUpperCase()) 
		{ 
	        case "SEVERE": 
	        	LEVEL = Level.SEVERE;
	        	break;
	        case "WARNING": 
	        	LEVEL = Level.WARNING;
	        	break;
	        case "INFO": 
	        	LEVEL = Level.INFO;
	        	break;
	        case "CONFIG": 
	        	LEVEL = Level.CONFIG;
	        	break;
	        case "FINE": 
	        	LEVEL = Level.FINE;
	        	break;
	        case "FINER": 
	        	LEVEL = Level.FINER;
	        	break;
	        case "FINEST": 
	        	LEVEL = Level.FINEST;
	        	break;
	        case "ALL": 
	        	LEVEL = Level.ALL;
	        	break;
	        case "OFF": 
	        	LEVEL = Level.OFF;
	        	break;
	        default: 
	        	LEVEL = Level.INFO;
 
        } 
		
		ch.setLevel(LEVEL);
        WinEventParser.logger.addHandler(ch);
        WinEventParser.logger.setLevel(LEVEL);
        logger.setUseParentHandlers(false);
	}	
}
