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
	int RANDOM_AMOUNT = 1000;
	
	private HTable table;
	private Path pt;
	private FileSystem fs;
	private BufferedReader br;

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
		int lineNo = (int) (Math.random() * 999);
		String result = null;
		try {
			result = this.br.readLine();
			for (int i = 0; i < lineNo; i++)
					result = this.br.readLine();
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		}
		catch (IOException e) {
				e.printStackTrace();
			}

		return result;
	}

	private String getKey(String line) {
		return line.split("\t")[0];
	}

	public int thousandRandom() {
		int count = 0;
		Result r;
		for (int i = 0; i < RANDOM_AMOUNT; i++) {
			r = getResults(getKey(getRandomLine()));
			if (!r.isEmpty())
				count++;
		}
		return count;
	}

	public int readAll() {
		String line;
		int count = 0;
		Result r;
		try {
			while ((line = this.br.readLine()) != null) {
				r = getResults(getKey(line));
				if (!r.isEmpty())
					count++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return count;
	}

	public static void main(String[] args) {
		Client c = new Client("index");
		/*
		 * For thousandRandom access print thousandRandmo
		 * For full 1000_popular_words iterator prin readAll
		 */
		//System.out.format("%d / 1000 returned\n", c.thousandRandom());
		System.out.format("%d / 1000 returned\n", c.readAll());
	}
}
