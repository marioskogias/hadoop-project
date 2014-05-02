
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ProcessResultsV2 {

	private String path;
	int successful;
	int unsuccessful;

	public ProcessResultsV2(String path) {
		if (path.substring(path.length() - 1).equals("/"))
			this.path = path;
		else
			this.path = path + "/";
	}

	private void extractInfo(String line) {
		String[] data = line.split("\t");
		if (data[0].equals("Succesfull:"))
			successful = Integer.parseInt(data[1]);
		else
			unsuccessful = Integer.parseInt(data[1]);
	}

	void getResults() {
		try {
			Path pt = new Path(this.path + "part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			this.extractInfo(line);
			line = br.readLine();
			this.extractInfo(line);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void printResults() {
		this.getResults();
		int sum = this.successful + this.unsuccessful;
		float sucRate = (this.successful / (float) sum) * 100;
		float unsucRate = (this.unsuccessful / (float) sum) * 100;

		DecimalFormat df = new DecimalFormat("#.##");
		System.out.format("Successful queries %d (%s %%)\n", this.successful,
				df.format(sucRate));
		System.out.format("Unccessful queries %d (%s %%)\n", this.unsuccessful,
				df.format(unsucRate));
	}

	/*
	 public static void main(String[] args) {
		ProcessResultsV2 r = new ProcessResultsV2("test_results");
		r.printResults();
	}
	*/
}
