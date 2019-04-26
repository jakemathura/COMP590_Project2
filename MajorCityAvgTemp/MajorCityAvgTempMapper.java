
// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MajorCityAvgTempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\\s+");

		double averageTemp = -10000.0;
		String date = tokens[0];
		String year = new String();
		int first_hyphen, first_slash;

		first_hyphen = date.indexOf('-');
		year = date.substring(1, first_hyphen);

		try {
			averageTemp = Double.parseDouble(tokens[1]);

		} catch (Exception e) {

		}

		context.write(new Text(year), new DoubleWritable(averageTemp));

	}
}
