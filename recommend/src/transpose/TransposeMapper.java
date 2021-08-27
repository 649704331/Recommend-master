package transpose;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransposeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text outKey = new Text();//输出的key值
	private Text outValue = new Text();//输出的value值
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] temp1 = value.toString().split("\t");
		System.out.println(temp1.length + temp1[0]);
		String row = temp1[0];//原来的行号，转化为列
		String[] temp2 = temp1[1].split(",");
		for(int i = 0; i < temp2.length; i++) {
			String column = temp2[i].split("_")[0];//列号，转化为行
			String tempValue = temp2[i].split("_")[1];//value值
			outKey.set(column);
			outValue.set(row + "_" + tempValue);
			context.write(outKey, outValue);
		}
		
	}
	
}
