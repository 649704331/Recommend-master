package reduce_result;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultReducer extends Reducer<Text, Text, Text, Text>{

	private Text outKey = new Text();//输出的key值	
	private Text outValue = new Text();//输出的value值
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		for(Text text : values) {
			sb.append(text + ",");
		}
		
		outKey.set(key);
		outValue.set(sb.toString().substring(0, sb.length() - 1));
		context.write(outKey, outValue);
	}
	
	
}
