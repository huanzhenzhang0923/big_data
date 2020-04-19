import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapper extends Mapper<Object, Text, Text, Text> {
    private int k;
    private double minDist;
    private int centerIndex;
    private List<ArrayList<Double>> centers;

//
//    @Override
//    public void setup(Context context) throws IOException{
//        centers = Assistance.getCenters(context.getConfiguration().get("centerpath"));
//        k = Integer.parseInt(context.getConfiguration().get("kpath"));
//        minDist = Double.MAX_VALUE;
//        centerIndex = 0;
//    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
        centers = Assistance.getCenters(context.getConfiguration().get("centerpath"));
        k = Integer.parseInt(context.getConfiguration().get("kpath"));
        minDist = Double.MAX_VALUE;
        centerIndex = 0;
        String line = value.toString();
        String[] fields = line.split(",");
        for (int i = 0; i < centers.size(); ++i){
            double currentDist = 0;
            for (int j = 0; j < fields.length; ++j){
                double tmp = Math.abs(centers.get(i).get(j) - Double.parseDouble(fields[j]));
                currentDist += Math.pow(tmp, 2);
            }
            if (currentDist<minDist ){
                minDist = currentDist;
                centerIndex = i;
            }
        }
        String val=value.toString()+","+1;
        Text centroids=new Text (centers.get(centerIndex).toString());
        context.write(centroids, new Text(val));
    }
}