package coursework2;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer1 extends Reducer<Text, TextFloatPair, Text, ArrayWritable> {

    public void reduce(Text key, Iterable<TextFloatPair> values, Context context)
        throws IOException, InterruptedException {
        
        ArrayList<TextFloatPair> ratings = new ArrayList<TextFloatPair>();

        for (TextFloatPair value : values) {
            ratings.add(value);
        }

        ArrayWritable outRatings = new ArrayWritable(TextFloatPair.class, ratings.toArray(new TextFloatPair[ratings.size()]));

        context.write(key, outRatings);
    }
}
