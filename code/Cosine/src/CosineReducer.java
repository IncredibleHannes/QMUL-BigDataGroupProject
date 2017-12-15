package coursework2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        float num = 0;
        float dem1 = 0;
        float dem2 = 0;
        double similarity = 0;
        String [] ratings = null;
        String [] moviesIds = key.toString().split(",");

        if (!moviesIds[0].equals(moviesIds[1])) {
          for (Text value : values) {
              ratings = value.toString().split(",");
              float rating1 = Float.parseFloat(ratings[0]);
              float rating2 = Float.parseFloat(ratings[1]);
              float average = Float.parseFloat(ratings[2]);
              num += (rating1 - average)*(rating2 - average);
              dem1 += Math.pow(rating1 - average, 2);
              dem2 += Math.pow(rating2 - average, 2);
          }
          similarity = num / (Math.sqrt(dem1)*Math.sqrt(dem2));
        }

        context.write(key, new DoubleWritable(similarity));
    }
}
