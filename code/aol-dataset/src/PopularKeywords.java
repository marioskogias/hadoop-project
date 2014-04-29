import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

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

public class PopularKeywords {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		FileInputStream fileStream;
		HashSet<String> stopWords = new HashSet<String>();
		
		/**
		 * add all the stop words in a HashSet for ease of use
		 */
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

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			String keywords[] = words[1].split(" ");
			for (String word : keywords) {
				if (!stopWords.contains(word))
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
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job,
				new Path("2_4_temp_results"));

		/* add destributed cache */
		DistributedCache.addCacheFile(
				new Path("/user/root/misc/english.stop").toUri(),
				job.getConfiguration());

		job.waitForCompletion(true);

		/* second m-r job */
		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "totalOrder");
		job2.setJarByClass(SearchCount.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setReducerClass(PKTotalOrder.Reduce.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setSortComparatorClass(DecreasingIntComparator.DecreasingComparator.class);

		job2.setPartitionerClass(IntTotalOrderPartitioner.class);
		job2.setNumReduceTasks(2);

		SequenceFileInputFormat
				.addInputPath(job2, new Path("2_4_temp_results"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);
	}

}