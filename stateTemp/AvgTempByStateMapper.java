import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTempByStateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");

		String state = new String();
		double temp = -10000.0;

		// get state
		state = tokens[4];
		try {
			temp = Double.parseDouble(tokens[1]);
		} catch (Exception e) {
		}
		context.write(new Text(state), new DoubleWritable(temp));

	}
}
