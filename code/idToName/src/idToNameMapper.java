package coursework2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class idToNameMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private HashMap<String,String> movieInfo;

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    //Lookup movieID in the cached database to get the movie name
    String[] fields = data.toString().split("\t");
    if (fields.length != 0) {
      String movieName = movieInfo.get(fields[0]);

      context.write(new Text(movieName), new DoubleWritable(Double.parseDouble(fields[1])));
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {

        //Load movie ID and Name from the "movies.dat" stored in cache
        movieInfo = new HashMap<String, String>();

        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        try {
            //discard the header row
            br.readLine();

            while ((line = br.readLine()) != null) {
                //context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);
               String[] fields = line.split("::");
                if (fields.length == 3)
                    movieInfo.put(fields[0], fields[1]);
            }
            br.close();
        }

        catch (IOException e1) {
        }

        super.setup(context);
    }
}
