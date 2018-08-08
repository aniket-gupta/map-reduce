package fragmadata.question1;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Stack;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top4TeamReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

	private static PriorityQueue<TossWinnerTeam> pq = new PriorityQueue<TossWinnerTeam>();

	@Override
	protected void reduce(NullWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		for (Text text : value) {
			String line = text.toString();
			String[] split = line.split(",");
			TossWinnerTeam team = new TossWinnerTeam(split[0], split[1], Integer.parseInt(split[2]));
			if (pq.size() < 4) {
				pq.offer(team);
			} else {
				TossWinnerTeam teamInPq = pq.peek();
				if (team.getCount() > teamInPq.getCount()) {
					pq.poll();
					pq.offer(team);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Stack<TossWinnerTeam> stack = new Stack<TossWinnerTeam>();
		while (!pq.isEmpty()) {
			stack.push(pq.poll());
		}

		while (!stack.isEmpty()) {
			TossWinnerTeam team = stack.pop();
			context.write(new Text(team.getYear() + "," + team.getName() + "," + team.getCount()), NullWritable.get());
		}

	}

}
