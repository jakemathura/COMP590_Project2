
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

public class LongitudeLatitudeTempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");

		double averageTemp = -10000.0;
		String latitude = tokens[6];
		String longitude = tokens[7];

		latitude = latitude.replaceAll("^\"|\"$", "");
		latitude = latitude.substring(0, latitude.length() - 1);
		longitude = longitude.replaceAll("^\"|\"$", "");
		longitude = longitude.substring(0, longitude.length() - 1);

		try {
			averageTemp = Double.parseDouble(tokens[2]);

		} catch (Exception e) {

		}

		String dateCityCountry = latitude + " " + longitude;

		context.write(new Text(dateCityCountry), new DoubleWritable(averageTemp));

	}
}
