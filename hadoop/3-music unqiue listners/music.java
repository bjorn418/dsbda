package Musicpackage;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Music{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =new  Job(conf, "Music Analytics");

        job.setJarByClass(Music.class);
        job.setMapperClass(MusicMapper.class);
        job.setReducerClass(MusicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input CSV path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

// ---------- Mapper ----------
public static class MusicMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("UserId")) return; // skip header
        String[] fields = value.toString().split(",");
        if (fields.length < 3) return;

        String userId = fields[0].trim();
        String trackId = fields[1].trim();
        String shared = fields[2].trim();

        context.write(new Text(trackId), new Text("USER:" + userId));

        if (shared.equals("1")) {
            context.write(new Text(trackId), new Text("SHARED"));
        }
    }
}

// ---------- Reducer ----------
public static class MusicReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueUsers = new HashSet<>();
        int shareCount = 0;
        for (Text val : values) {
            String v = val.toString();
            if (v.startsWith("USER:")) {
                uniqueUsers.add(v.substring(5));
            } else if (v.equals("SHARED")) {
                shareCount++;
            }
        }

        context.write(key, new Text("UniqueListeners: " + uniqueUsers.size() + ", SharedCount: " + shareCount));
    }
}


}
// 👉 Mapper (MusicMapper)
// It reads each line of the CSV.

// It skips the header line if present.

// It splits the line into fields: userId, trackId, and shared status.

// It emits the trackId as the key with:

// USER:userId as the value (to track listeners).

// Additionally, if the shared field equals 1, it also emits the same trackId with the value SHARED.

// 👉 Reducer (MusicReducer)
// It gets all values for each trackId.

// It maintains a set to store unique user IDs (to count unique listeners).

// It counts how many times the track was shared.

// Finally, it writes the trackId along with:

// The number of unique listeners.

// The number of times the track was shared.

// 🔥 2️⃣ Music1 Program (MusicUnique)
// 👉 Mapper (MapperClass)
// It reads each line of the CSV.

// It skips the header line if present.

// It splits the line into fields and extracts the userId.

// It emits the userId as the key with the value 1 (to count user activity).

// 👉 Reducer (ReducerClass)
// It receives all values for each userId.

// It sums up the values to count how many times each user appeared (number of listens/plays).

// It also keeps a counter of how many unique users there are in total.

// It writes:

// Each userId with their total count of plays.

// And at the end (in the cleanup step), it writes the total number of unique users.
