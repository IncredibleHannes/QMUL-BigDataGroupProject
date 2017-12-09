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

public class CosineReducer extends Reducer<TextTextPair, RatingInfo, TextTextPair, DoubleWritable> {

    public void reduce(TextTextPair key, Iterable<RatingInfo> values, Context context)
        throws IOException, InterruptedException {
        float num = 0;
        float dem1 = 0;
        float dem2 = 0;
        double similarity = 0;

        if (!key.getFirst().equals(key.getSecond())) {
          for (RatingInfo value : values) {
              float rating1 = value.getRating1().get();
              float rating2 = value.getRating2().get();
              float average = value.getAverage().get();
              num += (rating1 - average)*(rating2 - average);
              dem1 += Math.pow(rating1 - average, 2);
              dem2 += Math.pow(rating2 - average, 2);
          }
          similarity = num / (Math.sqrt(dem1)*Math.sqrt(dem2));
        }

        context.write(key, new DoubleWritable(similarity));
    }
}
