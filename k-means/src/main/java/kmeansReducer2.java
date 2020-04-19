import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.*;

public class kmeansReducer2 extends Reducer<Object, Text, Text, NullWritable> {
    private double sum;
    private long xvalue;
    private long yvalue;

    public void reduce(Object key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        sum = 0;
        xvalue = 0;
        yvalue = 0;
        for (Text val : value) {
            String line = val.toString();
            String[] fields = line.split(",");
            for (int i = 0; i < fields.length; ++i) {
                sum = sum + Double.valueOf(fields[2]);
                xvalue = xvalue + Long.valueOf(fields[0]);
                yvalue = yvalue + Long.valueOf(fields[1]);
            }
        }
        double xaverage = xvalue / sum;
        double yaverage = yvalue / sum;
        String tmpResult = xaverage + "," + yaverage;
        Text result = new Text(tmpResult);
        context.write(result,NullWritable.get());
    }
}