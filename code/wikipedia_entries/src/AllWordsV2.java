import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AllWordsV2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int queryCount = 0;
		
		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
					this.queryCount++;
				}
			System.out.println("The query count is "+ Integer.toString(this.queryCount));
			}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Text empty = new Text("");
			String title = value.toString();
			String[] words = title.split("\t");
			if (words.length == 1) {// this is a wiki file
				words = title.split("_");
				for (String word : words) {
					context.write(new Text(word), empty);
				}
			} else { // this is an aol file
				String search = words[1];
				String[] searchWords = search.split(" ");
				Text id = new Text(this.toString()+Integer.toString(this.queryCount));
				for (String word : searchWords) {
					context.write(new Text(word), id);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1); // this is for exists
		private IntWritable two = new IntWritable(2); // this is for not exists

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> cache = new ArrayList<String>();
			boolean found = false;
			for (Text v : values) {
				if (v.toString().equals(""))
					found = true;
				else
					cache.add(v.toString());
			}
			if (found)
				for (String v : cache) {
					context.write(new Text(v), one);
				}
			else
				for (String v : cache) {
					context.write(new Text(v), two);
				}
		}
	}

	public static void main(String[] args) throws Exception {

		// first job
		Configuration conf = new Configuration();

		Job job = new Job(conf, "allwords");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// aol dataset dir
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// wikipedia dataset dir
		FileInputFormat.addInputPath(job, new Path(args[1]));

		SequenceFileOutputFormat.setOutputPath(job, new Path("test_temp"));

		job.waitForCompletion(true); 

		// second job
		conf = new Configuration();
		job = new Job(conf, "count");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(CountQueriesV2.Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("test_temp"));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

		// process the files for the final results
		(new ProcessResultsV2(args[2])).printResults();

	}
}
