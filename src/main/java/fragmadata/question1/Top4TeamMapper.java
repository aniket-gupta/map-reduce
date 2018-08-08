package fragmadata.question1;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top4TeamMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static PriorityQueue<TossWinnerTeam> pq = new PriorityQueue<TossWinnerTeam>();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(",");
		TossWinnerTeam team = new TossWinnerTeam(split[0], split[1], Integer.parseInt(split[2]));
		if(pq.size() < 4) {
			pq.offer(team);
		} else {
			TossWinnerTeam teamInPq = pq.peek();
			if(team.getCount() > teamInPq.getCount()) {
				pq.poll();
				pq.offer(team);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		while (!pq.isEmpty()) {
			TossWinnerTeam team = pq.poll();
			context.write(NullWritable.get(), new Text(team.getYear() + "," + team.getName() + "," + team.getCount()));
		}
		
	}

	
	
	

}
