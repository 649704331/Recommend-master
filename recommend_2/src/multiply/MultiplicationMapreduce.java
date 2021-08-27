package multiply;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiplicationMapreduce {

	private static String input = "/recommend_2/similarity_output/";
	private static String output = "/recommend_2/multiply_output/";

	
	public boolean mr() {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS	", "hdfs://192.168.200.131:9000");
		
		try {
			Job job = Job.getInstance(configuration, "matrixMultiplication");
			//job.addCacheArchive(new URI(matrix2 + "#"));
			
			job.setJarByClass(MultiplicationMapreduce.class);
			job.setMapperClass(MultiplicationMapper.class);
			job.setReducerClass(MultiplicationReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem fs = FileSystem.get(configuration);
			Path inPath = new Path(input);
			if(fs.exists(inPath)) {
				FileInputFormat.addInputPath(job, inPath);
			}
			
			Path outPath = new Path(output);
			fs.delete(outPath,true);
			
			FileOutputFormat.setOutputPath(job, outPath);
			
			return job.waitForCompletion(true) ? true : false;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} /*catch (URISyntaxException e) {
			e.printStackTrace();
		}*/
		
		return false;
	}
	public static void main(String[] args) {
		MultiplicationMapreduce mmmr = new MultiplicationMapreduce();
		boolean res = mmmr.mr();
		if(res) {
			System.out.println("success");
		}
		else {
			System.out.println("fail");
		}
	}
}
