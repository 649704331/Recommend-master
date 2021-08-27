package transpose;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransposeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text outKey = new Text();//�����keyֵ
	private Text outValue = new Text();//�����valueֵ
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] temp1 = value.toString().split("\t");
		System.out.println(temp1.length + temp1[0]);
		String row = temp1[0];//ԭ�����кţ�ת��Ϊ��
		String[] temp2 = temp1[1].split(",");
		for(int i = 0; i < temp2.length; i++) {
			String column = temp2[i].split("_")[0];//�кţ�ת��Ϊ��
			String tempValue = temp2[i].split("_")[1];//valueֵ
			outKey.set(column);
			outValue.set(row + "_" + tempValue);
			context.write(outKey, outValue);
		}
		
	}
	
}
