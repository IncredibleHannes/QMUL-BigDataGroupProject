package coursework2;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CosineJob {
    public static void runJob(String[] input, String output) throws Exception {
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(CosineJob.class);
        job.setMapperClass(CosineMapper.class);
        job.setReducerClass(CosineReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RatingInfo.class);

        job.addCacheFile(new Path("/data/movie-ratings/movies.dat").toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
