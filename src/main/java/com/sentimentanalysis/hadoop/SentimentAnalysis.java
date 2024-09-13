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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
            // Load positive words from resources
            loadWordList("positive_words.txt", positiveWords);

            // Load negative words from resources
            loadWordList("negative_words.txt", negativeWords);
        }

        private void loadWordList(String fileName, Set<String> wordSet) throws IOException {
            // Use class loader to read the file from the classpath
            try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String word;
                while ((word = reader.readLine()) != null) {
                    wordSet.add(word.toLowerCase());
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int positiveCount = 0;
            int negativeCount = 0;

            while (itr.hasMoreTokens()) {
                String word = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", "");

                if (positiveWords.contains(word)) {
                    positiveCount++;
                }
                if (negativeWords.contains(word)) {
                    negativeCount++;
                }
            }

            if (positiveCount > negativeCount) {
                reviewClass.set("Positive");
                context.write(reviewClass, one);
            } else if (negativeCount > positiveCount) {
                reviewClass.set("Negative");
                context.write(reviewClass, one);
            }
        }
    }

    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
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

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
