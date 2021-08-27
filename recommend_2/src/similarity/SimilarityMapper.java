package similarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text outKey = new Text();//�����keyֵ
	private Text outValue = new Text();//�����valueֵ
	private ArrayList<String> matrix1 = new ArrayList<>();
	
	@Override
	protected void setup(Context context)//����matrix2����map����ִ��ǰִ�У�ִֻ��һ��
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path("/recommend_2/scoring_output/part-r-00000"));
		
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
		
		double s1 = 0;
		for(String str : temp1) {
			s1 += Double.parseDouble(str.split("_")[1]) * Double.parseDouble(str.split("_")[1]);
		}
		s1 = Math.sqrt(s1);
		
		for(String str:matrix1) {
			temp = str.toString().split("\t");
			String row2 = temp[0];
			String[] temp2 =  temp[1].split(",");
			
			double s2 = 0;
			for(String st : temp2) {
				s2 += Double.parseDouble(st.split("_")[1]) * Double.parseDouble(st.split("_")[1]);
			}
			s2 = Math.sqrt(s2);
			
			double result = 0;
			for(int i = 0;i < temp1.length;i++) {//������һ�к��Ҿ���ÿһ��������
//				System.out.println(temp1[i].split("_")[0] + temp1[i].split("_")[1]);
//				if(temp2.length == i) {
//					break;
//				}
				for(int j = 0;j < temp2.length;j++) {
					if(temp1[i].split("_")[0].equals(temp2[j].split("_")[0])) {
						result += Double.parseDouble(temp1[i].split("_")[1]) * Double.parseDouble(temp2[j].split("_")[1]);
					}
				}
			}
			double cos = result / (s1 * s2);
			if(cos == 0) {
				continue;
			}
			outKey.set(row1);
			outValue.set(row2 + "_" + new DecimalFormat("0.00").format(cos));
			context.write(outKey,outValue);
		}
	}

	
	
}
