import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Context;

public class WordCount {


    // --------------------------------------------------------------------------------
    // ------------------------------------ SETUP -------------------------------------
    // --------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------
    // ------------------------------------- MAP --------------------------------------
    // --------------------------------------------------------------------------------
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        //private final static IntWritable one = new IntWritable(1);
        // Variable to store text
        //private Text word = new Text();

        // Local hashmap to count occurrences of each word in split
        private HashMap<String, Integer> hm = new HashMap<>();
        private TreeMap<Integer, String> ltm = new TreeMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Take in the file one word/token at a time
            StringTokenizer itr = new StringTokenizer(value.toString());

            // Iterate through all tokens and get number of occurrences for each
            while (itr.hasMoreTokens()) {

                String s = itr.nextToken();

                // Add to hashset if not in
                if (!hm.containsKey(s)) { hm.put(s, 1); }

                // If exists, increment
                else {
                    int count = hm.get(s);
                    hm.put(s, count + 1);
                }
            }

            // Load entries into treemap for sorting
            for (String cur : hm.keySet()) {
                ltm.put(hm.get(cur), cur); // Add to treemap
                if (ltm.size() > 5) { ltm.pollLastEntry(); } // Remove any extra elements
            }

            // Write top 5 out to context
            for (Map.Entry<Integer, String> ent : ltm.entrySet()) {
                context.write(new Text(ent.getValue()), new IntWritable(ent.getKey())); // Write to context
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public TreeMap<Integer, String> tm;
        private IntWritable result = new IntWritable();

        protected void setup(Context context) throws IOException, InterruptedException {

            TreeMap<Integer, String> tm = new TreeMap<>(Collections.reverseOrder());

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // Get sum of values
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Find exact top 5 using shared treemap
            tm.put(sum, key.toString()); // Add to treemap
            if (tm.size() > 5) { tm.pollLastEntry(); } // Remove any extra elements past 5

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> ent : tm.entrySet()) {
                context.write(new Text(ent.getValue()), new IntWritable(ent.getKey()));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}