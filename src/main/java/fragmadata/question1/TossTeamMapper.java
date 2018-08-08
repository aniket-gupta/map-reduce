package fragmadata.question1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TossTeamMapper extends Mapper<LongWritable, Text, Text, Text> {

	// MATCH_ID,SEASON,CITY,DATE,TEAM1,TEAM2,TOSS_WINNER,TOSS_DECISION,RESULT,WINNER
	// 0		1		2	3	  4		5	  6			  7				8	  9
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(!line.startsWith("MATCH_ID")) {
			String[] split = line.split(",");
			String season = split[1];
			String tossWinner = split[6];
			String tossDecision = split[7];
			if((season.equals("2016") || season.equals("2017")) &&
					tossDecision.equalsIgnoreCase("field")) {
				context.write(new Text(season), new Text(tossWinner + ":1"));
			}
		}
	}

	
}
