import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.scraper.config.triggeredscraper.JobConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImpl;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.exception.ScraperException;
import org.apache.plc4x.java.scraper.triggeredscraper.TriggeredScraperImpl;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollector;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollectorImpl;
import org.apache.plc4x.java.utils.connectionpool.PooledPlcDriverManager;

public class Main {

	static Connection connection = null;
	public static void main(String[] args) {
		
		
		// Scraper: query scheduler
		ScraperConfigurationTriggeredImplBuilder schedluer = new ScraperConfigurationTriggeredImplBuilder();
		
		
		// JDBC driver name and database URL
	    String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
	    String url = "jdbc:iotdb://127.0.0.1:6667/";

	    // Database credentials
	    String username = "root";
	    String password = "root";

	    
	    try {
	      Class.forName(driver);
	      connection = DriverManager.getConnection(url, username, password);
	    } catch (ClassNotFoundException e) {
	      e.printStackTrace();
	    } catch (SQLException e) {
	      e.printStackTrace();
	    }
	    
	    
		// S7 connection string
		String connectionString = "s7://192.168.100.1";
		
		schedluer.addSource("s7Connection", connectionString);
		
		JobConfigurationTriggeredImplBuilder jobBuilder = schedluer.job("s7job", "(SCHEDULED,10000)");
		
		jobBuilder.source("s7Connection");
		jobBuilder.field("frequency", "%DB1.DB4.0:REAL");
		jobBuilder.build();
		ScraperConfigurationTriggeredImpl scraperConfig = schedluer.build();
		
		
		try {
			
			PlcConnection plcConnection = new PlcDriverManager().getConnection(connectionString);
			
			if (plcConnection.isConnected() == true) {
				
				System.out.println("PLC state: ONLINE");
				
			}
			
			else if(plcConnection.isConnected() == false) {
				
				System.out.println("PLC state: OFFLINE");
			}
			
			
			// Create a new read request:
			// - Give the single item requested an alias name
			PlcReadRequest.Builder builder = plcConnection.readRequestBuilder();
			// builder.addItem("value-1", "%Q0.0:BOOL");
			// builder.addItem("value-2", "%Q0:BYTE");
			// builder.addItem("value-3", "%I0.2:BOOL");
			builder.addItem("machine_state", "%DB1.DB0.0:INT");
			builder.addItem("start_cmd", "%DB1.DB2.0:BOOL");
			builder.addItem("stop_cmd", "%DB1.DB2.1:BOOL");
			builder.addItem("frequency", "%DB1.DB4.0:REAL");
			
			PlcReadRequest readRequest = builder.build();
			
			PlcReadResponse response = readRequest.execute().get(5000, TimeUnit.MILLISECONDS);
			System.out.println("machine_state: " + response.getObject("machine_state"));
			System.out.println("start_cmd: " + response.getObject("start_cmd"));
			System.out.println("stop_cmd: " + response.getObject("stop_cmd"));
			System.out.println("frequency: " + response.getObject("frequency"));
				
			
			Object value = (Float)response.getObject("frequency");
			
			// Insert into IoTDB
		    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO ? (TIMESTAMP, ?) VALUES (?, ?)")) {
	            statement.setString(1, "root.energy.pac2200");
	            statement.setString(2, "frequency");
	            statement.setLong(3, System.currentTimeMillis());
	            statement.setFloat(4,(float) value);
	            statement.execute();
	        } catch (SQLException e) {
	            System.out.println(e.toString());
	        }
		    
		    
		    try {
	            PlcDriverManager plcDriverManager = new PooledPlcDriverManager();
	            TriggerCollector triggerCollector = new TriggerCollectorImpl(plcDriverManager);
	            TriggeredScraperImpl scraper = new TriggeredScraperImpl(scraperConfig, (jobName, sourceName, results) -> {
	            	
	            	try {
	            		
						PlcReadResponse responsePolling = readRequest.execute().get(5000, TimeUnit.MILLISECONDS);
						System.out.println("frequency polling: " + responsePolling.getObject("frequency"));
						
						// Insert into IoTDB - ToDo
						
						Object valueP = (Float)responsePolling.getObject("frequency");
						
						// Insert into IoTDB
					    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO ? (TIMESTAMP, ?) VALUES (?, ?)")) {
				            statement.setString(1, "root.energy.pac2200");
				            statement.setString(2, "frequency");
				            statement.setLong(3, System.currentTimeMillis());
				            statement.setFloat(4,(float) valueP);
				            statement.execute();
				        } catch (SQLException e) {
				            System.out.println(e.toString());
				        }
						
					} catch (InterruptedException | ExecutionException | TimeoutException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	           
	            }, triggerCollector);
	            scraper.start();
	            triggerCollector.start();
	        } catch (ScraperException e) {
	            System.out.println("Error starting the scraper");
	        }
		    
		   
		    
			
		}catch (Exception e) {
			
			System.out.println(e.toString());
		}
		
		
		
		
		
		


	}
	
	
	

}