package fragmadata.question3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10BowlerMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	static Map<String, PriorityQueue<TopBowler>> map = new HashMap<>();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		String year = split[0];
		String bowler = split[1];
		Float economy = new Float(split[2]);
		TopBowler topBowler = new TopBowler(bowler, economy);
		if(!map.containsKey(year)) {
			map.put(year, new PriorityQueue<>());
		}
		PriorityQueue<TopBowler> pq = map.get(year);
		if(pq.size() < 10) {
			pq.offer(topBowler);
		} else {
			TopBowler bowlerInPq = pq.peek();
			if(bowlerInPq.compareTo(topBowler) > 0) {
				pq.poll();
				pq.offer(topBowler);
			}
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		for(String year : map.keySet()) {
			PriorityQueue<TopBowler> pq = map.get(year);
			while (!pq.isEmpty()) {
				TopBowler bowler = pq.poll();
				context.write(new Text(year), new Text(bowler.toString()));
			}
		}

		
		
	}

	

	
	
}
