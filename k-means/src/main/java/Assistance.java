import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import java.io.File;

import java.io.IOException;
import java.util.*;

public class Assistance {
    public static List<ArrayList<Double>> getCenters(String inputpath){
        List<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
        Configuration conf = new Configuration();
        try {
            Path in = new Path(inputpath);
            FileSystem hdfs = in.getFileSystem(conf);
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            ArrayList<Double>  center = null;
            while (lineIn.readLine(line) > 0){
                String record = line.toString();
                center = new ArrayList<Double>();
                String[] fields = record.split(",");
                //List<Float> tmplist = new ArrayList<Float>();
                for (int i = 0; i < fields.length; ++i){
                    center.add(Double.parseDouble(fields[i]));
                }
                result.add(center);
            }
            fsIn.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }

    public static void deleteLastResult(String path){
        Configuration conf = new Configuration();
        try {
            Path path1 = new Path(path);
            FileSystem hdfs = path1.getFileSystem(conf);
            hdfs.delete(path1, true);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    public static boolean isFinished(String oldpath, String newpath, int k, float threshold)
            throws IOException{
        List<ArrayList<Double>> oldcenters = Assistance.getCenters(oldpath);
        List<ArrayList<Double>> newcenters = Assistance.getCenters(newpath);
        float distance = 0;
        int dimension=oldcenters.get(0).size();
        int oldcentersize=oldcenters.size();
        int newcentersize=newcenters.size();
//        System.out.println(oldcentersize);
//        System.out.println(newcentersize);
        System.out.println("cluster:"+k);
        System.out.println("dimension:"+dimension);



        for (int i = 0; i < k; ++i){
            for (int j = 0; j <dimension; ++j){
                double tmp = Math.abs(oldcenters.get(i).get(j) - newcenters.get(i).get(j));
                distance += Math.pow(tmp, 2);
            }
        }
        System.out.println("Distance = " + distance + " Threshold = " + threshold);
        if (distance < threshold){
            System.out.println("we successfully achieve the convergence");
            return true;
        }

        Assistance.deleteLastResult(oldpath);
        Configuration conf = new Configuration();
        Path path0 = new Path(newpath);
        FileSystem hdfs=path0.getFileSystem(conf);
        hdfs.copyToLocalFile(new Path(newpath), new Path("/home/huanzhen/Desktop/temp/temp"));
        hdfs.delete(new Path(oldpath), true);
        hdfs.moveFromLocalFile(new Path("/home/huanzhen/Desktop/temp/temp"), new Path(oldpath));
        return false;
    }
}