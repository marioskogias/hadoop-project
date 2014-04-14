import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class IndexLoad {

	static String family = "articles";

	public static class Map extends TableMapper<Text, Text> {

		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {

			// create value first
			String original = key.toString();
			String md5Text = "";
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("MD5");
				md.update(original.getBytes());
				byte[] digest = md.digest();
				StringBuffer sb = new StringBuffer();
				for (byte b : digest) {
					sb.append(String.format("%02x", b & 0xff));
				}
				md5Text = sb.toString();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			Text article = new Text(md5Text);

			String s = value.toString();
			String[] words = s.split("_");

			for (String word : words) {
				context.write(new Text(word), article);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		HBaseConfiguration.addHbaseResources(conf);

		Job job = new Job(conf, "indexload");
		job.setJarByClass(IndexLoad.class);

		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

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

		HTable hTable = new HTable(job.getConfiguration(), "content");

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		job.waitForCompletion(true);

		/*
		 * After that just run bin/hadoop jar lib/hbase-0.94.17.jar
		 * completebulkload /user/root/hbase context to add the new rows to the
		 * table
		 */

	}

}