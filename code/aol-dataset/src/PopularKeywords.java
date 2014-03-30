import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PopularKeywords {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			String keywords[] = words[1].split(" ");
			for (String word : keywords) {
				context.write(new Text(word), one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, IntWritable, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(new IntWritable(count), key);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "poplarKeywords");
		job.setJarByClass(SearchCount.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("project_temp/popularSites"));

		job.waitForCompletion(true);
		
		/*second m-r job*/
		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "totalOrder");
		job2.setJarByClass(SearchCount.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setMapperClass(PKTotalOrder.Map.class);
		job2.setReducerClass(PKTotalOrder.Reduce.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setSortComparatorClass(DecreasingIntComparator.DecreasingComparator.class);
		
		job2.setPartitionerClass(IntTotalOrderPartitioner.class);
		job2.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job2, new Path("project_temp/popularSites"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);
	}

}