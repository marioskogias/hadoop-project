/**
 * 2.2 success-failure rate
 * Calculate successful vs failed queries rate
 */

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Rates {

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable success = new IntWritable(1);
		private final static IntWritable failure = new IntWritable(0);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			if (words.length == 3) // failure
				context.write(one, failure);
			else
				context.write(one, success);
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, IntWritable, Text, Text> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int success = 0;
			int failure = 0;
			for (IntWritable val : values) {
				if (val.get() == 1)
					success += 1;
				else 
					failure += 1;
			}
			int sum = success + failure;
			double missRate = 100 * (failure/(double)sum);
			double hitRate = 100 * (success/(double)sum);
			
			DecimalFormat df = new DecimalFormat("#.##");
			Text label = new Text();
			Text rate = new Text();
			
			label.set("Successful searches (%): ");
			rate.set(df.format(hitRate));
			context.write(label, rate);
			
			label.set("Unsuccessful searches (%): ");
			rate.set(df.format(missRate));
			context.write(label, rate);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "rates");
		job.setJarByClass(SearchCount.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}