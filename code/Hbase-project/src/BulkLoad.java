import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class BulkLoad {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text title = new Text();
		NullWritable none = NullWritable.get();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			title.set(line);
			context.write(title, none);
		}
	}

	public static class Reduce extends Reducer<Text, NullWritable, Text, Text> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			String original = key.toString();
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("MD5");
				md.update(original.getBytes());
				byte[] digest = md.digest();
				StringBuffer md5Text = new StringBuffer();
				for (byte b : digest) {
					md5Text.append(String.format("%02x", b & 0xff));
				}
				
				context.write(key, new Text(md5Text.toString()));
				
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			

		}
	}

	public static void main(String[] args) throws Exception {
	}

}