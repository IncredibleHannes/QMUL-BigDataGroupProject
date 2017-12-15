package coursework2;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RecommendationsJob {
    public static void runJob(String[] input, String output, String userRatingsFile) throws Exception {
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(RecommendationsJob.class);
        job.setMapperClass(RecommendationsMapper.class);
        job.setReducerClass(RecommendationsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path("/data/movie-ratings/movies.dat").toUri());
        job.addCacheFile(new Path(userRatingsFile).toUri()); // output file of UserRatings where target user is located: e.g. out/part-r-00000

        // for testing purposes
        //job.addCacheFile(new Path("testInput/testMovies").toUri());
        //job.addCacheFile(new Path(userRatingsFile/*"testInput/testData"*/).toUri());

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 2), args[args.length - 2], args[args.length - 1]);
    }
}
