import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BulkLoad {

	static String family = "wikipedia";
	static String qualifier = "titles";

	public static class Map extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// create key as md5 of the value
			String original = value.toString();
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

			hKey.set(md5Text.getBytes());

			// create the column value
			kv = new KeyValue(hKey.get(), family.getBytes(),
					qualifier.getBytes(), value.toString().getBytes());

			context.write(hKey, kv);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		HBaseConfiguration.addHbaseResources(conf);

		Job job = new Job(conf, "bulkload");
		job.setJarByClass(BulkLoad.class);

		job.setNumReduceTasks(2);

		job.setMapperClass(Map.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		HTable hTable = new HTable(job.getConfiguration(), "content");
	    
	    // Auto configure partitioner and reducer
	    HFileOutputFormat.configureIncrementalLoad(job, hTable);
	    
		job.waitForCompletion(true);
		
		/*
		 * After that just run 
		 * bin/hadoop jar lib/hbase-0.94.17.jar completebulkload /user/root/hbase content
		 * to add the new rows to the table
		 */

	}

}