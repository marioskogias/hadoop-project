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
				words = title.split("_");
				for (String word : words) {
					context.write(new Text(word), two);
					// context.write(new Text(word.replaceAll("[^a-zA-Z ]",
					// "").toLowerCase()), two); // this removes all punctuation
				}
			} else { // this is an aol file
				String search = words[1];
				String[] searchWords = search.split(" ");
				for (String word : searchWords) {
					context.write(new Text(word), one);
					// context.write(new Text(word.replaceAll("[^a-zA-Z ]",
					// "").toLowerCase()), one); // this removes all punctuation
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, IntWritable, IntWritable> {

		private IntWritable one = new IntWritable(1); // this is for exists
		private IntWritable two = new IntWritable(2); // this is for not exists

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int ones = 0;
			int twos = 0;
			for (IntWritable el : values) {
				if (el.get() == 1)
					ones++;
				else
					twos++;
			}
			if (twos > 0)
				context.write(one, new IntWritable(ones));
			else
				context.write(two, new IntWritable(ones));
		}
	}

	public static void main(String[] args) throws Exception {

		//first job 
		Configuration conf = new Configuration();

		Job job = new Job(conf, "allwords");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//aol dataset dir
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//wikipedia dataset dir
		FileInputFormat.addInputPath(job, new Path(args[1]));
		
		SequenceFileOutputFormat.setOutputPath(job, new Path("2_6_temp_results"));

		job.waitForCompletion(true);

		//second job
		conf = new Configuration();
		job = new Job(conf, "count");
		job.setJarByClass(AllWords.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(CountQueries.Reduce.class);
		job.setNumReduceTasks(2);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("2_6_temp_results"));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
		
		// process the files for the final results
		(new ProcessResults(args[2])).printResults();
		

	}
}
