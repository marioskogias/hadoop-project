import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountQueriesV2 {
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			int count_pos = 0;
			int count_neg = 0;
			boolean found = false;
			while (context.nextKey()) {
				for (IntWritable v : context.getValues())
					if (v.get() == 1) {
						found = true;
						break;
					}
				if (found)
					count_pos++;
				else
					count_neg++;
				found = false;
			}
			context.write(new Text("Succesfull:"), new IntWritable(count_pos));
			context.write(new Text("Unsuccesfull:"), new IntWritable(count_neg));
		}
	}
}
