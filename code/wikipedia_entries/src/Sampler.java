import java.io.IOException;

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
			while (context.nextKeyValue()) {
				if (count1 == step) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
					count1 = -1;
					count2++;
					if (count2 == target)
						break;
				}
				count1++;
			}
		}
	}

}