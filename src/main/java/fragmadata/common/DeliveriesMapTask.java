package fragmadata.common;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeliveriesMapTask extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(",");
		System.out.println("DeliveriesMapTask: " + line);
		if (!line.startsWith("MATCH_ID")) {
			context.write(new IntWritable(Integer.parseInt(split[0])), new Text(line.substring(line.indexOf(",") + 1)));
		}
	}

}
