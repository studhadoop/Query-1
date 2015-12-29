import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author sreeveni
 * @date
 * 
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	NullWritable out = NullWritable.get();
	Text valEmit = new Text();

	public void map(LongWritable key, Text value, Context context) {
		Configuration conf = context.getConfiguration();
		/*
		 * For eg lets use arraylist to store data
		 */
		ArrayList arrayList = new ArrayList();
		String line = value.toString();
		String[] parts = line.split(",");
		for (int i = 0; i < parts.length; i++) {
			arrayList.add(parts[i]);
		}
		/*
		 * Sort arrayList
		 */
		Collections.sort(arrayList);
		/*
		 * iterate through arraylist to get desired result
		 */
		String emitdata = "";
		for (int i = 0; i < arrayList.size(); i++) {
			if (i % 2 == 0) {
				if (i == 0) {
					emitdata = (String) arrayList.get(i);
				} else {
					emitdata += "," + arrayList.get(i);
				}
			}
		}
		System.out.println(emitdata);
		valEmit.set(emitdata);
		try {
			context.write(valEmit, out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
