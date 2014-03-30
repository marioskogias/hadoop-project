import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PKTotalOrder {

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			context.write(new IntWritable(Integer.parseInt(words[0])),
					new Text(words[1]));
		}
	}

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