import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author sreeveni
 *
 */
public class Driver extends Configured implements Tool {

	public static void main(String[] args)  {
		Configuration conf = new Configuration();
		int res = 0;
		try {
			res = ToolRunner.run(conf, new Driver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		System.exit(res);
	}

	public int run(String[] args)  {
		// TODO Auto-generated method stub
		String source = args[0];
		String dest = args[1];
		runIdentity(source,dest);
		return  0;
	}

	/*
	 * Run identity mapper and Reducer
	 */
	public static void runIdentity(String source, String dest) {
		// TODO Auto-generated method stub
		Path in = new Path(source);
		Path out = new Path(dest);
		Configuration conf=new Configuration();

		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		/*
		 * Run identity mapper and Reducer
		 */
		Job job = null;
		try {
			job = new Job(conf," ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		job.setJarByClass(Driver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		try {
			TextInputFormat.addInputPath(job, in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		TextOutputFormat.setOutputPath(job, out);
		try {
			if(fs.exists(out)){
				fs.delete(out, true);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean success = false;
		try {
			success = job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}

	}
}
