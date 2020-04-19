import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Combiner extends Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException{
        List<ArrayList<Double>> assistList = new ArrayList<ArrayList<Double>>();
        String tmpResult = "";
        for (Text val : value){
            String line = val.toString();
            String[] fields = line.split(",");
            List<Double> tmpList = new ArrayList<Double>();
            for (int i = 0; i < fields.length; ++i){
                tmpList.add(Double.parseDouble(fields[i]));
            }
            assistList.add((ArrayList<Double>) tmpList);
        }
        long count=assistList.size();
        for (int i = 0; i < assistList.get(0).size(); ++i){
            long sum = 0;
            for (int j = 0; j < assistList.size(); ++j){
                sum += assistList.get(j).get(i);
            }
            long tmp = sum;
            if (i == 0){
                tmpResult = tmpResult+tmp;
            }
            else{
                tmpResult = tmpResult+ "," + tmp;
            }
        }
        tmpResult=tmpResult+","+count;
        Text result = new Text(tmpResult);
        context.write(key,result);
    }
}