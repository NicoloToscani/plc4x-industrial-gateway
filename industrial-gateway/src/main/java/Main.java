import java.io.Console;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;

public class Main {

	public static void main(String[] args) {
		
		String connectionString = "s7://192.168.100.1";
		
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
			builder.addItem("actual_temperature", "%DB1.DB4.0:REAL");
			
			PlcReadRequest readRequest = builder.build();
			
			PlcReadResponse response = readRequest.execute().get(5000, TimeUnit.MILLISECONDS);
			System.out.println("machine_state: " + response.getObject("machine_state"));
			System.out.println("start_cmd: " + response.getObject("start_cmd"));
			System.out.println("stop_cmd: " + response.getObject("stop_cmd"));
			System.out.println("actual_temperature: " + response.getObject("actual_temperature"));
			
			
			
		}catch (Exception e) {
			
			System.out.println(e.toString());
		}
		
		
		


	}

}