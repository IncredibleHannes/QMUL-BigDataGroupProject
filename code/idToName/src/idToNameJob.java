package coursework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class idToNameJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setJarByClass(idToNameJob.class);
        job.setMapperClass(idToNameMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(new Path("/data/movie-ratings/movies.dat").toUri());

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(),
                new idToNameJob(), args);
        System.exit(res);
    }
}
