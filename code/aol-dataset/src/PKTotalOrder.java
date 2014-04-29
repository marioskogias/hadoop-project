import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PKTotalOrder {

	public static class Reduce extends
			Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text keyword : values) {
				context.write(new Text(keyword), key);
			}
		}
	}
}