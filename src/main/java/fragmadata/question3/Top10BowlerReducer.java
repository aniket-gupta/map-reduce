package fragmadata.question3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10BowlerReducer extends Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<TopBowler> list = new ArrayList<>();
		for (Text text : values) {
			list.add(TopBowler.fromString(text.toString()));
		}
		Collections.sort(list);

		for (int i = list.size() - 1; i >= list.size() - 10; i--) {
			context.write(new Text(key.toString() + "," + list.get(i).toString()), NullWritable.get());
		}

	}

}
