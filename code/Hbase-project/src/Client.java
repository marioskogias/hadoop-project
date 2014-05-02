import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Client {

	String FAMILY = "articles";
	String PATH = "/user/root/misc/1000_popular_keywords";

	private HTable table;
	private Path pt;
	private FileSystem fs;
	private BufferedReader br;

	// String line;
	// line = br.readLine();
	public Client(String tableName) {

		Configuration config = HBaseConfiguration.create();
		pt = new Path(PATH);
		try {
			this.table = new HTable(config, tableName);
			fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public Result getResults(String keyword) {
		Get g = new Get(Bytes.toBytes(keyword));
		Result r = null;
		try {
			r = this.table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

	private String getRandomLine() {
		int lineNo = (int) (Math.random() * 1000);
		String result = null;
		for (int i = 0; i < lineNo; i++)
			try {
				result = this.br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}

		// reset reader
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	private String getKey(String line) {
		System.out.println(line);
		return line.split("\t")[0];
	}

	public int thousandRandom() {
		int count = 0;
		Result r;
		for (int i = 0; i < 1000; i++) {
			r = getResults(getKey(getRandomLine()));
			if (!r.isEmpty())
				count++;
		}
		return count;
	}

	public static void main(String[] args) {
		Client c = new Client("index");
		System.out.format("%d / 1000 returned\n", c.thousandRandom());
	}
}
