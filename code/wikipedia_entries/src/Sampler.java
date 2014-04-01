import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Sampler {

	public static class Map extends LexiSort.Map {
		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);

			/*
			 * Sample every step input lines
			 * 
			 * Collect target samples per mapper
			 */
			int step = 50;
			int count1 = 0; // count1 step
			int count2 = 0; // count2 target
			int target = 50; // this is the amount we want
			while (context.nextKeyValue() && (count2 < target))  {
				if (count1 == step) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
					count1 = -1;
					count2++;
				}
				count1++;
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		
		int MAPPERS_COUNT = 2; 			// use these if counters 
		int SAMPLES_PER_MAPPER = 100;  	//are not updated yet
		
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
			System.out.println("There are "+ Integer.toString(reducersNo)+ "reducers");
			samples = context.getCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"MAP_OUTPUT_RECORDS").getValue();
			if (samples == 0)
				samples = MAPPERS_COUNT * SAMPLES_PER_MAPPER;
			step = (int) samples / (reducersNo - 1);
			
			System.out.println("Step = "+ Integer.toString(step));
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