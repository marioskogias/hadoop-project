import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountQueries {
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count=0;
			for (IntWritable el : values) 
				count += el.get();			
			context.write(key, new IntWritable(count));
		}
	}
}
