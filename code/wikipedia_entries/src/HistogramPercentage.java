import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class HistogramPercentage {

	public static String category(int c) {
		switch (c) {
		case 1:
			return "[0...9]";
		case 2:
			return "A*";
		case 3:
			return "B*";
		case 4:
			return "C*";
		case 5:
			return "D*";
		case 6:
			return "E*";
		case 7:
			return "F*";
		case 8:
			return "G*";
		case 9:
			return "H*";
		case 10:
			return "I*";
		case 11:
			return "J*";
		case 12:
			return "K*";
		case 13:
			return "L*";
		case 14:
			return "M*";
		case 15:
			return "N*";
		case 16:
			return "O*";
		case 17:
			return "P*";
		case 18:
			return "Q*";
		case 19:
			return "R*";
		case 20:
			return "S*";
		case 21:
			return "T*";
		case 22:
			return "U*";
		case 23:
			return "V*";
		case 24:
			return "W*";
		case 25:
			return "X*";
		case 26:
			return "Y*";
		case 27:
			return "Z*";
		default:
			return "~!@#$%^&*()_+{}|:\"<>?[];',./";
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String val = value.toString();
			context.write(one, new Text(val));
		}
	}

	/* consider one single reducer */
	public static class Reduce extends
			Reducer<NullWritable, Text, Text, Text> {

		Text k = new Text();
		Text val = new Text();
		int[] arr = new int[28];
		
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text v : values) {
				String[] elements = v.toString().split("_");
				arr[Integer.parseInt(elements[0])] = Integer.parseInt(elements[1]);
				count += Integer.parseInt(elements[1]);
			}

			for (int i=0;i<28;i++) {
				double percentage = 100 * arr[i]/(double) count;
				DecimalFormat df = new DecimalFormat("#.##");
				val.set(df.format(percentage) + " %");
				k.set(category(i));
				context.write(k, val);
			}

		}
	}

}
