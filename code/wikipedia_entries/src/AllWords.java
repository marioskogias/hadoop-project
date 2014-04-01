import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AllWords {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1); // this is for aol
		private IntWritable two = new IntWritable(2); // this is for wikipedia

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String title = value.toString();
			String[] words = title.split("\t");
			if (words.length == 1) {// this is a wiki file
				words = title.split("\t");
				for (String word : words) {
					context.write(new Text(word), two);
					// context.write(new Text(word.replaceAll("[^a-zA-Z ]",
					// "").toLowerCase()), two); // this removes all punctuation
				}
			} else { // this is an aol file
				for (String word : words) {
					context.write(new Text(word), one);
					// context.write(new Text(word.replaceAll("[^a-zA-Z ]",
					// "").toLowerCase()), one); // this removes all punctuation
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, IntWritable, NullWritable> {

		private IntWritable one = new IntWritable(1); // this is for exists
		private IntWritable two = new IntWritable(2); // this is for not exists

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			boolean foundOne = false;
			boolean foundTwo = false;
			for (IntWritable el : values) {
				if (el.get() == 1)
					foundOne = true;
				else
					foundTwo = true;
				if (foundOne && foundTwo)
					break;
			}
			if (foundOne && foundTwo)
				context.write(one, NullWritable.get());
			else
				context.write(two, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "allwords");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path("temp"));

		job.waitForCompletion(true);

		conf = new Configuration();
		job = new Job(conf, "count");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(CountQueries.Reduce.class);
		job.setNumReduceTasks(2);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("temp"));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
