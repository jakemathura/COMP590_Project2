import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class StateOverTimeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");

		String date = new String();
		String year = new String();
		String century = new String();
		String state = new String();
		String stateplusdate = new String();
		double avgLandTemp = -10000.0;
		int first_hyphen, first_slash;

		// get year
		date = tokens[0];
		state = tokens[3];
		stateplusdate = "unvalid";
		if(date.length() == 10) {
			first_hyphen = date.indexOf('-');
			year = date.substring(0, first_hyphen);
		} else {
			first_slash = date.lastIndexOf('/');

			if(tokens.length == 10) {
				year = "20" + date.substring(first_slash + 1, date.length());
			} else {
				year = "19" + date.substring(first_slash + 1, date.length());
			}
		}

		century = year.substring(0,2) + "00";

		if((Integer.parseInt(year) - Integer.parseInt(century)) <= 49) {
		} else {
			century = year.substring(0,2) + "50";
		}


		try {
			avgLandTemp = Double.parseDouble(tokens[1]);
		} catch (Exception e) {
		}
		stateplusdate = state +"\t" +century;

		context.write(new Text(stateplusdate), new DoubleWritable(avgLandTemp));

	}
}
