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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

	public static class Reduce extends
			Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {

		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			//set the key
			hKey.set(key.getBytes());
			int i=0;
			for (Text val : values) {
				// create the column value
				String qualifier = Integer.toString(i);
				kv = new KeyValue(hKey.get(), family.getBytes(),
						qualifier.getBytes(), val.getBytes());
				context.write(hKey, kv);
				i++;
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "indexload");
		job.setJarByClass(IndexLoad.class);

		job.setNumReduceTasks(2);
		job.setReducerClass(Reduce.class);
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
				Text.class, // mapper output key
				Text.class, // mapper output value
				job);

		FileOutputFormat.setOutputPath(job, new Path("hbase_index"));
		
		job.waitForCompletion(true);

		/*
		 * After that just run 
		 * bin/hadoop jar lib/hbase-0.94.17.jar completebulkload /user/root/hbase index
		 * to add the new rows to the
		 * table
		 */

	}

}