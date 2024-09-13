package com.sentimentanalysis.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class SentimentAnalysis {

    public static class SentimentMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Set<String> positiveWords = new HashSet<>();
        private final Set<String> negativeWords = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable negOne = new IntWritable(-1);
        private final Text reviewClass = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load positive words from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                loadWordList(cacheFiles[0], positiveWords);
                loadWordList(cacheFiles[1], negativeWords);
            }
        }

        private void loadWordList(URI path, Set<String> wordSet) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            String word;
            while ((word = reader.readLine()) != null) {
                wordSet.add(word.toLowerCase());
            }
            reader.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setCombinerClass(SentimentReducer.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add positive and negative word lists to the distributed cache
        job.addCacheFile(new URI("/input/dataset/positive_words.txt"));
        job.addCacheFile(new URI("/input/dataset/negative_words.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
