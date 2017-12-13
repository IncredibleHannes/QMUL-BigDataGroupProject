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
        
        //Compare ratings of both movies and count how many times they have recieved the same rating and how many times they 
        //have recieved different ratings
        double sameRatings = 0.0;
        double totalRatings = movie1.size() + movie2.size();

        for (float f : movie1){
          if(movie2.contains(f)){
            sameRatings += 1.0;
            movie2.remove(f);
          }
        }

        double jaccard = sameRatings/totalRatings;

        context.write(key, new DoubleWritable(jaccard));
    }
}
