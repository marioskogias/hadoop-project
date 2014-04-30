import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class Sampler {

	static int SAMPLES_PER_MAPPER = 1000;

	public static class Map extends LexiSort.Map {
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
	}

	public static class Reduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		int MAPPERS_COUNT = 2; // use this with SAMPLES_PER_MAPPER if counters
								// are not updated yet

		int reducersNo;
		long samples;
		int count = 0; // count of split steps
		int step; // emit a key-value every step key-value pairs
		int counter = 0; // count of key-value pairs passed

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			reducersNo = Integer.parseInt(context.getConfiguration().get(
					"REDUCERS_NO"));
			System.out.println("There are " + Integer.toString(reducersNo)
					+ " reducers");
			/*
			 * samples = context.getCounter(
			 * "org.apache.hadoop.mapred.Task$Counter",
			 * "MAP_OUTPUT_RECORDS").getValue();
			 */
			Counter cnter = context.getCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"MAP_OUTPUT_RECORDS");
			samples = cnter.getValue();
			System.out.println("There are " + Long.toString(samples)
					+ " samples");
			// if (samples == 0)
			// samples = MAPPERS_COUNT * SAMPLES_PER_MAPPER;
			step = (int) samples / (reducersNo - 1);
			System.out.println("Step = " + Integer.toString(step));
		}

		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);

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