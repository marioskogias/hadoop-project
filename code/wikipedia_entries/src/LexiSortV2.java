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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class LexiSortV2 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		FileInputStream fileStream;
		HashSet<String> stopWords = new HashSet<String>();

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
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		int numReduceTasks = 10;

		Configuration conf = new Configuration();

		Job job = new Job(conf, "sampling");
		job.setJarByClass(LexiSort.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(SamplerV2.Map.class);
		job.setReducerClass(SamplerV2.Reduce.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		SequenceFileOutputFormat.setOutputPath(job, new Path(
				"/user/root/2_5_2_partition"));

		/* add destributed cache */
		DistributedCache.addCacheFile(
				new Path("/user/root/misc/english.stop").toUri(),
				job.getConfiguration());
		job.getConfiguration().set("REDUCERS_NO",
				Integer.toString(numReduceTasks));
		job.waitForCompletion(true);

		/* second hadoop jop after sampling */

		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "lexisort");
		job2.setJarByClass(LexiSort.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(NullWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

		job2.setMapperClass(Map.class);
		job2.setReducerClass(Reduce.class);

		job2.setNumReduceTasks(numReduceTasks);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		/* add destributed cache */
		DistributedCache.addCacheFile(
				new Path("/user/root/misc/english.stop").toUri(),
				job2.getConfiguration());

		Path inputDir = new Path("/user/root/2_5_2_partition");
		Path partitionFile = new Path(inputDir, "part-r-00000");
		TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(),
				partitionFile);
		job2.setPartitionerClass(TotalOrderPartitioner.class);

		job2.waitForCompletion(true);

	}
}