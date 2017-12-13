package coursework2;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JaccardReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        //For each movie pair, store the ratings for both movies in arrays
        ArrayList<Float> movie1 = new ArrayList<Float>();
        ArrayList<Float> movie2 = new ArrayList<Float>();
        
        for (Text t : values){
          String [] ratings = t.toString().split(",");
          movie1.add(Float.parseFloat(ratings[0]));
          movie2.add(Float.parseFloat(ratings[1]));
        }
        
        //Compare ratings of both movies and count how many times the same rating appears in both sets (intersection)
        // ArrayList<Float> intersection = new ArrayList<Float>(movie1);
        // intersection.retainAll(movie2);
        // double sameRatings = intersection.size();

        ArrayList<Float> intersection = new ArrayList<Float>();
        ArrayList<Float> tempMovie2 = new ArrayList<Float>(movie2);

        for (Float f : movie1){
          if (tempMovie2.contains(f)){
            intersection.add(f);
            tempMovie2.remove(f);
          }
        }

        double sameRatings = intersection.size();
        double totalRatings = movie1.size() + movie2.size();


        double jaccard = sameRatings/totalRatings;

        context.write(key, new DoubleWritable(jaccard));
    }
}
