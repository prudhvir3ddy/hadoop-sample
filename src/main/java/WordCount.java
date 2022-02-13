import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class WordCount {

    public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            String[] words = value.toString().split("\\s+");

            HashMap<String, Integer> hashMap = new HashMap<>();

            for (int i = 0; i < words.length; i++) {
                String[] copy = Arrays.copyOfRange(words, i, words.length);
                if (copy.length >= 5) {
                    String s = findWord5(copy);
                    if (hashMap.containsKey(s)) {
                        hashMap.put(s, hashMap.get(s) + 1);
                    } else {
                        hashMap.put(s, 1);
                    }
                }
            }

            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                Text text = new Text();
                text.set(entry.getKey());
                IntWritable intWritable = new IntWritable();
                intWritable.set(entry.getValue());
                output.collect(text, intWritable);
            }
        }
    }

    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(WordCount.class);
        job.setJobName("word_count");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setJarByClass(WordCount.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileUtils.deleteDirectory(new File("/Users/Z006XLL/IdeaProjects/hadoop-sample/src/main/java/output3"));
        FileInputFormat.addInputPath(job, new Path("/Users/Z006XLL/IdeaProjects/dsa-problem-solving/src/com/prudhvireddy/Sample.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/Z006XLL/IdeaProjects/hadoop-sample/src/main/java/output3"));

    }

    public static String findWord5(String[] words) {
        HashSet<String> hashSet = new HashSet<>();
        int count = 0;
        int i = 0;
        StringBuilder word5 = new StringBuilder();
        while (count < 5 && i < words.length) {
            if (!hashSet.contains(words[i])) {
                word5.append(words[i]).append(" ");
                count++;
            }
            hashSet.add(words[i]);
            i++;
        }
        return word5.toString();
    }
}
