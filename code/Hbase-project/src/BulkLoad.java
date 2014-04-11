import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class BulkLoad {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text title = new Text();
		NullWritable none = NullWritable.get();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			title.set(line);
			context.write(title, none);
		}
	}

	public static class Reduce extends
			Reducer<Text, NullWritable, Text, Text> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {
	}

}