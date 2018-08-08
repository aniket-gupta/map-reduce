package fragmadata.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoiningReduceTask extends Reducer<IntWritable, Text, Text, NullWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String session = null;
		int matchID = key.get();
		List<String> list = new ArrayList<String>();
		for (Text text : values) {
			String value = text.toString();
			String[] split = value.split(",");
			if(split.length > 1) {
				list.add(value);
			} else {
				session = value;
			}
			
		}

//		if (session != null) {
//			for (Text text : values) {
//				String value = text.toString();
//				
//				String out = matchID + "," + session + "," + value;
//				context.write(new Text(out), NullWritable.get());
//			}
//		} else {
//			context.write(new Text("session is null"), NullWritable.get());
//			for (Text text : values) {
//				context.write(text, NullWritable.get());
//			}
//		}
		
		for(String str : list) {
			String out = matchID + "," + session + "," + str;
			context.write(new Text(out), NullWritable.get());
		}
		

	}

}
