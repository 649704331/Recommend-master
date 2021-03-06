package matrixMultiplication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text outKey = new Text();//输出的key值
	private Text outValue = new Text();//输出的value值
	private ArrayList<String> matrix1 = new ArrayList<>();
	
	@Override
	protected void setup(Context context)//读入matrix2，在map方法执行前执行，只执行一次
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path("/matrix/matrixTranspose_output/part-r-00000"));
		
		//FileReader fileReader = new FileReader("/matrix/matrixTranspose_output/part-r-00000");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String temp = null;
		while(true) {
			temp = br.readLine();
			if(temp == null) {
				break;
			}
			matrix1.add(temp);
		}
		in.close();
		br.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value,  Context context)
			throws IOException, InterruptedException {

		String[] temp = value.toString().split("\t");
		String row1 = temp[0];
		String[] temp1 = temp[1].split(",");
		for(String str:matrix1) {
			temp = str.toString().split("\t");
			String row2 = temp[0];
			String[] temp2 =  temp[1].split(",");
			int result = 0;
			for(int i = 0;i < temp1.length;i++) {//左矩阵第一行和右矩阵每一行相乘求和
				result += Integer.parseInt(temp1[i].split("_")[1]) * Integer.parseInt(temp2[i].split("_")[1]);
			}
			outKey.set(row1);
			outValue.set(row2 + "_" + result);
			context.write(outKey,outValue);
		}
	}

	
	
}
