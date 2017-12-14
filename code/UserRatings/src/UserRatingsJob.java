package coursework2;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class UserRatingsJob {
    public static void runJob(String[] input, String output) throws Exception {
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(UserRatingsJob.class);
        job.setMapperClass(UserRatingsMapper.class);
        job.setReducerClass(UserRatingsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextFloatPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextFloatPair.class);

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
