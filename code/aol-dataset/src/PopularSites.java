/**
 * 2.3 Popular Websites
 * Find sites with more than 10 unique visits
 */
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PopularSites {

	/*
	 * <web_site, username>
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			try{
				context.write(new Text(words[4]), new IntWritable(Integer.parseInt(words[0])));
			} catch (IndexOutOfBoundsException e) {}
			catch (NumberFormatException e) {};
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			HashSet<Integer> users = new HashSet<Integer>();
			for (IntWritable val : values) {
				users.add(val.get());
				}
				if (users.size() > 10) {
					context.write(key, new IntWritable(users.size()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "popularSites");
		job.setJarByClass(SearchCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}