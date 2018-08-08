package fragmadata.question3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OverAndRunCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		
		// context.write(new Text(matchId + "," + year + "," + bowler + "," + over), new IntWritable(totalRun));
		// context.write(new Text(key.toString() + "," + sum), NullWritable.get());
		String year = split[1];
		String bowler = split[2];
		context.write(new Text(year + "," + bowler), new IntWritable(Integer.parseInt(split[4])));
		
	}

}
