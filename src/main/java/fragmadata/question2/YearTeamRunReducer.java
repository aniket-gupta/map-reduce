package fragmadata.question2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YearTeamRunReducer extends Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		String run = iterator.next().toString();
		String[] split = run.split(":");
		TeamRun teamRun = new TeamRun(Integer.parseInt(split[0]), Integer.parseInt(split[1]),
				Integer.parseInt(split[2]));
		while (iterator.hasNext()) {
			String str = iterator.next().toString();
			String[] runs = str.split(":");
			teamRun.add(new TeamRun(Integer.parseInt(runs[0]), Integer.parseInt(runs[1]), Integer.parseInt(runs[2])));
		}
		
		String[] sessionTeam = key.toString().split(":");
		String session = sessionTeam[0];
		String team = sessionTeam[1];
		String fours = String.valueOf(teamRun.getFour());
		String six = String.valueOf(teamRun.getSix());
		String totalRun = String.valueOf(teamRun.getTotalRun());
		context.write(new Text(session + "," + team + "," +fours + "," + six + "," + totalRun), NullWritable.get());
	}

}
