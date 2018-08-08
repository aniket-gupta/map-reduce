package fragmadata.question1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TossTeamReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(Text text : values) {
			String[] split = text.toString().split(":");
			String teamName = split[0];
			int count = Integer.parseInt(split[1]);
			if(map.containsKey(teamName)) {
				count += map.get(teamName);
			}
			map.put(teamName, count);
		}
		
		for(String str : map.keySet()) {
			context.write(new Text(String.valueOf(key.get()) + "," + str + "," + String.valueOf(map.get(str))), NullWritable.get());
		}
	}
	
	

}
