import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;


public class Client {

	private HTable table;
	
	public Client(String tableName) {
		
		Configuration config = HBaseConfiguration.create();
		try {
			this.table = new HTable(config, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
