package scoring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ScoringReducer extends Reducer<Text, Text, Text, Text>{

	private Text outKey = new Text();//输出的key值	
	private Text outValue = new Text();//输出的value值
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String item = key.toString();
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(Text text : values) {
			String username = text.toString().split("_")[0];
			String score = text.toString().split("_")[1];
			if(map.get(username) == null) {
				map.put(username, Integer.parseInt(score));
			}
			else {
				int temp = map.get(username);
				map.put(username, temp + Integer.parseInt(score));
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Integer> entry:map.entrySet()) {
			String username = entry.getKey();
			int score = entry.getValue();
			sb.append(username + "_" + score + ","); 
		}
		outKey.set(item);;
		outValue.set(sb.toString().substring(0, sb.length() - 1));
		context.write(outKey, outValue);
	}
	
	
}
