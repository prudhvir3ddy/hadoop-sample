import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountTest {

    public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(WordCountTest.class);
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(WC_Mapper.class);
        conf.setCombinerClass(WC_Reducer.class);
        conf.setReducerClass(WC_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileUtils.deleteDirectory(new File("/Users/Z006XLL/IdeaProjects/hadoop-sample/src/main/java/output3"));

        FileInputFormat.setInputPaths(conf, new Path("/Users/Z006XLL/IdeaProjects/hadoop-sample/src/main/java/sample.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("/Users/Z006XLL/IdeaProjects/hadoop-sample/src/main/java/output3"));
        JobClient.runJob(conf);
    }
}
