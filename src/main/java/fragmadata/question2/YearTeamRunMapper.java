package fragmadata.question2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearTeamRunMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(!line.startsWith("MATCH_ID")) {
			String[] split = line.split(",");
			if(split.length == 17) {
				String session = split[1];
				String teamName = split[3];
				String totalRun = split[split.length - 1];
				int four = 0;
				int six = 0;
				int runScored = Integer.parseInt(split[split.length - 3]);
				if(runScored == 4) {
					four = 1;
				} else if (runScored == 6) {
					six = 1;
				}
				String runVal = String.valueOf(four) + ":" + String.valueOf(six) + ":" + totalRun;
				context.write(new Text(session + ":" + teamName), new Text(runVal));
			}
		}
	}
	
	

}
