package fragmadata.question3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OverBowlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(",");
		String matchId = split[0];
		String year = split[1];
		String over = split[5];
		String bowler = split[8];
		int byeRun = Integer.parseInt(split[10]);
		int legbyRun = Integer.parseInt(split[11]);
		int totalRun = Integer.parseInt(split[split.length - 1]);
		totalRun = totalRun -  legbyRun - byeRun;
		context.write(new Text(matchId + "," + year + "," + bowler + "," + over), new IntWritable(totalRun));
	}
	
	

}
