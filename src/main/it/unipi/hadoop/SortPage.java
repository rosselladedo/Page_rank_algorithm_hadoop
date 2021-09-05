package it.unipi.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;
import java.util.LinkedList;
import java.util.List;
import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import static org.junit.Assert.assertEquals;

public class SortPage {

    private String input_file = "";
    private String output_file = "";

    public SortPage(String input, String output) {input_file = input; output_file = output;}

    public static class SortPageMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            //leggiamo una serie di nomepagina:::pagerank:::lista

            while (itr.hasMoreTokens()){
                String token = itr.nextToken();
                String page_name = token.split(":::")[0];
                String page_rank = token.split(":::")[1];

                DoubleWritable outputKey = new DoubleWritable(Double.parseDouble(page_rank));
                Text outputValue = new Text(page_name);

                context.write(outputKey, outputValue);
            }
        }
    }

    public static class SortPageReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text>{
        public void reduce(DoubleWritable key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }

    public boolean run() throws Exception {
        //final String output = baseOutput + OUTPUT_PATH;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":::"); // set OUTPUT_SEPARATOR as separator
        //conf.set("stream.map.output.field.separator", "::");
        // instantiate job
        final Job job = new Job(conf, "SortPage");
        job.setJarByClass(SortPage.class);
        //job.setNumReduceTasks(5);

        // set mapper/reducer
        job.setMapperClass(SortPageMapper.class);
        job.setReducerClass(SortPageReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        // define reducer's output key-value
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(input_file));
        FileOutputFormat.setOutputPath(job, new Path(output_file));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSortComparatorClass(CompositeKeyComparator.class);

        return job.waitForCompletion(true);
    }
}



