package Moviepackage;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Movie {
    public static void main(String[] args) throws Exception {
        // Initialize the Hadoop configuration and job
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "Movie Rating Average");

        // Set the Jar class for the job
        job.setJarByClass(Movie.class);
        
        // Set Mapper and Reducer classes
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        // Set input and output file paths
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input file (CSV)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output directory

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

// Mapper Class
public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header row
        if (key.get() == 0 && value.toString().contains("userId")) return;

        String[] fields = value.toString().split(",");
        if (fields.length < 3) return;  // Skip lines that don't have enough data
        
        try {
            // Extract movieId and rating from the CSV
            int movieId = Integer.parseInt(fields[1].trim()); // Movie ID (index 1)
            float rating = Float.parseFloat(fields[2].trim()); // Rating (index 2)

            // Write the movieId and rating to context
            context.write(new IntWritable(movieId), new FloatWritable(rating));
        } catch (NumberFormatException e) {
            // Ignore malformed lines
        }
    }
}

// Reducer Class
public static class MovieReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        // Summing up the ratings and counting the total number of ratings
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        // Calculate average and write the output
        if (count > 0) {
            float avg = sum / count;
            context.write(key, new FloatWritable(avg));  // Output the movieId and average rating
        }
    }
}


}