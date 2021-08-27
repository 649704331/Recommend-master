package scoring;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoringMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text outKey = new Text();//�����keyֵ
	private Text outValue = new Text();//�����valueֵ
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String username = value.toString().split(",")[0];
		String item = value.toString().split(",")[1];
		String score = value.toString().split(",")[2];
		
		outKey.set(username);
		outValue.set(item + "_" + score);
		
		context.write(outKey, outValue);
	}
	
	
}
