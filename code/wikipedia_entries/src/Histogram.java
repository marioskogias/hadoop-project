import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.io.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Histogram {

	static int SAMPLES_PER_MAPPER = 50;
	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		FileInputStream fileStream;
		HashSet<String> stopWords = new HashSet<String>();
		int limit = 0;

		public void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			fileStream = new FileInputStream(localFiles[0].toString());
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fileStream));
			String line = reader.readLine();
			while (line != null) {
				stopWords.add(line);
				line = reader.readLine();
			}

		}

		public int categorize(String s) {
			int v = (int) Character.toLowerCase(s.toCharArray()[0]);
			if ((v > 47) && (v < 58)) // digit
				return 1;
			else if ((v > 122) || (v < 97)) // symbol
				return 0;
			else
				return (v % 97) + 2; // letter
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String title = value.toString();
			String[] words = title.split("_");
			for (String word : words)
				if (!stopWords.contains(word))
					context.write(new IntWritable(categorize(word)), one);
		}

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);

			/*
			 * In order to produce less key-values use the following
			 */
			float step = (float) 1.0 / SAMPLES_PER_MAPPER;
			float counter = 0;
			while (context.nextKeyValue() && (counter < 1)) {
				if (context.getProgress() > counter) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
					counter += step;
				}
			}
			
			/*
			 * Else comment out the previous and use the following
			 * for full output 
			 */
			/*while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}*/
		}
	}

	/* consider one single reducer */
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, NullWritable, Text> {

		Text t = new Text();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable v : values)
				count++;
			t.set(Integer.toString(key.get()) + "_" + Integer.toString(count));
			context.write(NullWritable.get(), t);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "histogram");
		job.setJarByClass(Histogram.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(28);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(
				"2_5_1_temp_results"));

		/* add destributed cache */
		DistributedCache.addCacheFile(
				new Path("/user/root/misc/english.stop").toUri(),
				job.getConfiguration());

		job.waitForCompletion(true);

		/* we assume a single reducer */
		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "histogramPercentage");
		job2.setJarByClass(HistogramPercentage.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// job2.setMapperClass(HistogramPercentage.Map.class);
		job2.setReducerClass(HistogramPercentage.Reduce.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job2, new Path(
				"2_5_1_temp_results"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);
	}

}