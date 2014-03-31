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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class LexiSort {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {
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

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String title = value.toString();
			String[] words = title.split("_");
			for (String word : words)
				if (!stopWords.contains(word))
					context.write(new Text(word), NullWritable.get());
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, IntWritable, Text, NullWritable> {


		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
					context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		
		int numReduceTasks = 28;
		Configuration conf = new Configuration();

		Job job = new Job(conf, "lexisort");
		job.setJarByClass(LexiSort.class);

		//job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/* add destributed cache */
		DistributedCache.addCacheFile(
				new Path("/user/root/misc/english.stop").toUri(),
				job.getConfiguration());

		job.waitForCompletion(true);
	}
}