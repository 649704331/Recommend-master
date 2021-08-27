package matrixTranspose;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixTransposeMapreduce {

	private static String input = "/matrix/matrixTranspose_input/matrix2.txt";
	private static String output = "/matrix/matrixTranspose_output";
	
	public boolean mr() {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS	", "hdfs://192.168.200.131:9000");
		
		try {
			Job job = Job.getInstance(configuration, "matrixTranspose");
			job.setJarByClass(MatrixTransposeMapreduce.class);
			job.setMapperClass(MatrixTransposeMapper.class);
			job.setReducerClass(MatrixTransposeReducer.class);
			
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
		}
		
		return false;
	}
	
	public static void main(String[] args) {
		boolean res = false;
		res = new MatrixTransposeMapreduce().mr();
		if(res) {
			System.out.println("success");
		}
		else {
			System.out.println("fail");
		}
	}
}
