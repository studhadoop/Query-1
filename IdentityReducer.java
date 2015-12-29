import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author sreeveni
 */
public class IdentityReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {
	NullWritable out = NullWritable.get();

	public void reduce(Text key, Iterable<NullWritable> values, Context context) {

		try {
			context.write(key, out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
