import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountQueries {
	public static class Reduce extends
			Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			int count=0;
			for (NullWritable el : values) 
				count++;			
			context.write(key, new IntWritable(count));
		}
	}
}
