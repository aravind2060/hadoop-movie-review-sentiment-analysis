package com.sentimentanalysis.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * Sentiment Analysis program using Hadoop MapReduce.
 * 
 * This program reads text files, analyzes the sentiment of each review,
 * and outputs the sentiment classification along with review details.
 */
public class SentimentAnalysis {

    /**
     * Mapper class responsible for sentiment analysis of individual reviews.
     */
    public static class SentimentMapper extends Mapper<Object, Text, Text, Text> {

        // Sets to store positive and negative words
        private final Set<String> positiveWords = new HashSet<>();
        private final Set<String> negativeWords = new HashSet<>();

        /**
         * Setup method called once before mapping.
         * Loads positive and negative word lists from resources.
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load positive words from resources
            loadWordList("positive_words.txt", positiveWords);

            // Load negative words from resources
            loadWordList("negative_words.txt", negativeWords);
        }

        /**
         * Loads word list from file into a set.
         * 
         * @param fileName File name of word list
         * @param wordSet  Set to store words
         */
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

        /**
         * Maps review text to sentiment classification.
         * 
         * @param key   Input key (not used)
         * @param value Review text
         * @param context Mapper context
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Convert review text to string
            String review = value.toString();

            // Tokenize review text into words
            StringTokenizer itr = new StringTokenizer(review);

            // Initialize counters for positive and negative words
            int positiveCount = 0;
            int negativeCount = 0;

            // Sets to store found positive and negative words
            Set<String> positiveWordsFound = new HashSet<>();
            Set<String> negativeWordsFound = new HashSet<>();

            // Iterate through each word in review
            while (itr.hasMoreTokens()) {
                // Clean word by removing non-alphabetic characters and converting to lowercase
                String word = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z-]", "");

                // Check if word is positive
                if (positiveWords.contains(word)) {
                    positiveCount++;
                    positiveWordsFound.add(word);
                }

                // Check if word is negative
                if (negativeWords.contains(word)) {
                    negativeCount++;
                    negativeWordsFound.add(word);
                }
            }

            // Determine sentiment classification based on word counts
            String sentimentClass;
            if (positiveCount > negativeCount) {
                sentimentClass = "Positive";
            } else if (negativeCount > positiveCount) {
                sentimentClass = "Negative";
            } else {
                sentimentClass = "Neutral";
            }

            // Construct output value with review and sentiment details
            StringBuilder outputValue = new StringBuilder();
            outputValue.append("Review: ").append(review);
            outputValue.append("\tPositive words: ").append(positiveWordsFound);
            outputValue.append("\tNegative words: ").append(negativeWordsFound);

            // Write sentiment classification and review details to output
            context.write(new Text(sentimentClass), new Text(outputValue.toString()));
        }
    }

    /**
     * Reducer class responsible for aggregating sentiment analysis results.
     */
    public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Reduces sentiment analysis results.
         * 
         * @param key    Sentiment classification
         * @param values Review details
         * @param context Reducer context
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Initialize count for sentiment classification
            int count = 0;

            // Output each review detail
            for (Text val : values) {
                context.write(key, val);
                count++;
            }
            // Output total count for sentiment classification
            context.write(key, new Text("Total count: " + count));
        }
    }

    /**
     * Main class responsible for configuring and running the MapReduce job.
     * 
     * @param args Command-line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("Input Path: " + args[0]);
        System.out.println("Output Path: " + args[1]);
        
        // Check if required arguments are provided
        if (args.length < 2) {
            System.err.println("Usage: SentimentAnalysis <input_path> <output_path>");
            System.exit(2);
        }
        

        // Create Hadoop configuration
        Configuration conf = new Configuration();

        // Create MapReduce job
        Job job = Job.getInstance(conf, "Sentiment Analysis");

        // Set jar class
        job.setJarByClass(SentimentAnalysis.class);

        // Set mapper class
        job.setMapperClass(SentimentMapper.class);

        // Remove combiner as we are dealing with Text values
        // job.setCombinerClass(SentimentReducer.class);

        // Set reducer class
        job.setReducerClass(SentimentReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}