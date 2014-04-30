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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sampler {

	static int SAMPLES_PER_MAPPER = 1000;

	/**
	 * 
	 * This mapper will emit <Text, IntWritable> for normal words intwritable
	 * will be zero At the end a "" text with IntWritable = pair_counts will be
	 * emited so that the reducer will be able to get pairs amount
	 * 
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		int pair_count = 0;
		IntWritable zero = new IntWritable(0);
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
			context.write(new Text(""), new IntWritable(pair_count));
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String title = value.toString();
			String[] words = title.split("_");
			for (String word : words)
				if (!stopWords.contains(word)) {
					context.write(new Text(word), zero);
					pair_count++;
				}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, NullWritable> {

		int reducersNo;
		long samples;
		int count = 0; // count of split steps
		int step; // emit a key-value every step key-value pairs
		int counter = 0; // count of key-value pairs passed

		public void run(Context context) throws IOException,
				InterruptedException {
			// setup(context);
			int samples = 0;
			context.nextKey();
			if (context.getCurrentKey().toString().equals("")) {
				Iterable<IntWritable> values = context.getValues();
				for (IntWritable val : values) {
					samples += val.get();
				}
			}
			
			//there are definately common words so cat in the middle
			samples = samples / 2;
			reducersNo = Integer.parseInt(context.getConfiguration().get(
					"REDUCERS_NO"));
			step =  samples / (reducersNo - 1);
			System.out.format("There are %d reducers and %d samples",reducersNo,samples);
			
			//skip the first
			for (int i=0;i<step;i++)
				context.nextKey();
					
			while (count < (reducersNo - 1) && context.nextKey()) {
				if ((counter % step) == 0) {
					context.write(context.getCurrentKey(), NullWritable.get());
					count++;
				}
				counter++;
			}
		}
	}
}