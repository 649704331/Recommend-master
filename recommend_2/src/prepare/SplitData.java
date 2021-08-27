package prepare;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
 
public class SplitData {
	
	static Set<Integer> userids=new TreeSet<>();
	
	static TreeMap<Integer, Integer> idrows=new TreeMap<>();
	
	static HashMap<Integer, TreeMap<Integer, String>> idrowidrows=new HashMap<>();
	
	public void getdata(String path) throws NumberFormatException, IOException{
		
		FileInputStream inputStream=new FileInputStream(path);
		BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while((line=reader.readLine())!=null){
			String[] str=line.split(",");
			int userid=Integer.parseInt(str[0]);
			userids.add(userid);
			if (!idrows.containsKey(userid)) {
				
				idrows.put(userid,1);
				TreeMap<Integer, String> map=new TreeMap<>();
				map.put(1, line);
				idrowidrows.put(userid, map);
				
			}else {
				int count=idrows.get(userid)+1;
				idrows.put(userid, count);
				
				TreeMap<Integer, String> map=idrowidrows.get(userid);
				map.put(count, line);
				idrowidrows.put(userid, map);
			}
			
		}		
		reader.close();
	}
	
	
	public void splitData(double ratio,String path,String path1) throws IOException {
		
		OutputStream outputStream=new FileOutputStream(path);
		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(outputStream));
		
		OutputStream outputStream1=new FileOutputStream(path1);
		BufferedWriter writer1=new BufferedWriter(new OutputStreamWriter(outputStream1));
		
		for (Integer userid : userids) {
							
				int rows=idrows.get(userid);
				int testrows=rows-(int) (rows*ratio);
				Set<Integer> ir=randomSet(1, rows, testrows, new HashSet<Integer>());
				for (Integer rowid : ir) {
					String row=idrowidrows.get(userid).get(rowid);
					writer.write(row);
					writer.newLine();
					idrowidrows.get(userid).remove(rowid);
				}
		}
		writer.close();
		outputStream.close();
		
		for (Integer userid : userids) {
			for (Map.Entry<Integer, String> useridrows : idrowidrows.get(userid).entrySet()) {
					writer1.write(useridrows.getValue());
					writer1.newLine();
				
				}
			}
		writer1.close();
		outputStream1.close();
	}
	
 
    public static Set<Integer> randomSet(int min, int max, int n, HashSet<Integer> set) {
        if (n > (max - min + 1) || max < min) {
            return set;
        }
        for (int i = 0; i < n; i++) {
            int num = (int) (Math.random() * (max - min)) + min;
            set.add(num);
        }
        int setSize = set.size();
        if (setSize < n) {
            randomSet(min, max, n - setSize, set);
        }
		return set;
    }
    public static void main(String[] args) {
    	double ratio=0.7;//»®·Ö±ÈÀý
		String path="action.txt";
		String testpath="test3.txt";
		String trainpath="train7.txt";
		try {
			new SplitData().getdata(path);
			new SplitData().splitData(ratio, testpath, trainpath);
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
	}
    
}
