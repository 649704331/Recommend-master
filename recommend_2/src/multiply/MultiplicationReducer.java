package multiply;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplicationReducer extends Reducer<Text, Text, Text, Text>{

	private Text outKey = new Text();//输出的key值	
	private Text outValue = new Text();//输出的value值

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		ArrayList<String> str = new ArrayList<>();
		for(Text text:values) {
				str.add(text.toString());
		}
		Collections.sort(str);
		for(String s:str) {
			sb.append(s + ",");
		}
		String temp = sb.toString();
		temp = temp.substring(0, temp.length() - 1);
		
		outKey.set(key);
		outValue.set(temp);
		
		context.write(outKey, outValue);
	}
}
