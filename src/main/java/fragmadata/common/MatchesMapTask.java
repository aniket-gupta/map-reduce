package fragmadata.common;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatchesMapTask extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("MatchesMapTask: " + line);
		String[] split = line.split(",");
		if (!line.startsWith("MATCH_ID")) {
			context.write(new IntWritable(Integer.parseInt(split[0])), new Text(split[1]));
		}

	}
}
