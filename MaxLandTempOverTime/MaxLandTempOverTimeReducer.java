// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxLandTempOverTimeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                        throws IOException, InterruptedException {

                double total = 0;
                double avg = 0;
                long count = 0;
                double val;

                // iterate through all the values (count == 1) with a common key
                for (DoubleWritable value : values) {
                        val = value.get();

                        if(val > -9999) {
                                total += val;
                                count += 1;
                        }
                }
                avg = total / count;

                context.write(key, new DoubleWritable(avg));
        }
}
