import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SamplerV2 {

	static int SAMPLES_PER_MAPPER = 50;

	/**
	 * 
	 * This mapper will emit <Text, IntWritable> for normal words intwritable
	 * will be zero At the end a "" text with IntWritable = pair_counts will be
	 * emited so that the reducer will be able to get pairs amount
	 * 
	 */
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

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);

			float step = (float) 1.0 / SAMPLES_PER_MAPPER;
			float counter = 0;
			while (context.nextKeyValue() && (counter < 1)) {
				if (context.getProgress() > counter) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
					counter += step;
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String title = value.toString();
			String[] words = title.split("_");
			for (String word : words)
				if (!stopWords.contains(word)) {
					context.write(new Text(word), NullWritable.get());
				}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, NullWritable> {

		public void run(Context context) throws IOException,
				InterruptedException {

			int reducersNo = Integer.parseInt(context.getConfiguration().get(
					"REDUCERS_NO"));
			float step = (float) 1.0 / (reducersNo-1);
			float counter = 0;
			int my_counter = 0;
			while (context.nextKeyValue() && (counter < 1)) {
				if (context.getProgress() > counter) {
					context.write(context.getCurrentKey(), NullWritable.get());
					counter += step;
					my_counter++;
				}
			}
			System.out.println("Created "+Integer.toString(my_counter)+ " pairs");
		}
	}
}