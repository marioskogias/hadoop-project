import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexLoad {

	static String family = "articles";

	public static class Map extends
			TableMapper<ImmutableBytesWritable, KeyValue> {

		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;

		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {

			// create value first
			String article =Bytes.toString(key.get());

			byte[] val = value.getValue(Bytes.toBytes("wikipedia"),
					Bytes.toBytes("titles"));
			// If we convert the value bytes, we should get back 'Some Value',
			// the
			// value we inserted at this location.
			String valueStr = Bytes.toString(val);
			String[] words = valueStr.split("_");

			// qualifier will be the last 8 chars of the article md5
			String qualifier = article.substring(24);
			
			for (String word : words) {
				hKey.set(word.getBytes());
				kv = new KeyValue(hKey.get(), family.getBytes(),
						qualifier.getBytes(), valueStr.getBytes());
				context.write(hKey, kv);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "indexload");
		job.setJarByClass(IndexLoad.class);

		job.setOutputFormatClass(HFileOutputFormat.class);

		/*
		 * Input format is an hbase table
		 */
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("content", // input HBase table
															// name
				scan, // Scan instance to control CF and attribute selection
				Map.class, // mapper
				ImmutableBytesWritable.class, // mapper output key
				KeyValue.class, // mapper output value
				job);

		FileOutputFormat.setOutputPath(job, new Path("3_2_results"));

		HTable hTable = new HTable(job.getConfiguration(), "index");

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		job.waitForCompletion(true);

		/*
		 * After that just run 
		 * bin/hadoop jar lib/hbase-0.94.17.jar completebulkload /user/root/3_2_results index 
		 * to add the new rows to the
		 * table
		 */

	}

}