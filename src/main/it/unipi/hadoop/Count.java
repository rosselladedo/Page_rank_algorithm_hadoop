package it.unipi.hadoop;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File; 
import java.io.FileNotFoundException;  
import java.util.Scanner;
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
import java.io.IOException;
import java.util.StringTokenizer;

public class Count {

    private String input_file = "";
    private String output_file = "";

    public Count(String input, String output) {input_file = input; output_file = output;}

    public int pageNumber() {
        int count = 0;
        String str = "";
        String findStr = "<title>";
        int lastIndex = 0;
        //str = Files.readString(java.nio.file.Paths.get(input_file));

        try {

            Scanner scanner = new Scanner(new File(input_file));
            str = scanner.useDelimiter("\\Z").next();
            scanner.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        while(lastIndex != -1){

            lastIndex = str.indexOf(findStr,lastIndex);

            if(lastIndex != -1){
                count ++;
                lastIndex += findStr.length();
            }
        }

        return count;
    }

    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        private IntWritable num = new IntWritable();
        private Text title = new Text();
        private Text links = new Text();
        private String titolo = "";
        List<String> out_links = new LinkedList<>();
        String token = "";

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()){

                token = itr.nextToken();
                titolo = getTitle(token);
                title.set(titolo);

                out_links = getLinks(token);
                if(!(out_links == null)){
                    for(String link : out_links){
                        links.set(link);
                        context.write(title, links);
                    }
                }
                else
                    context.write(title, new Text(""));
            }
        }
    }

    public static class CountReducer extends Reducer<Text,Text,Text,Text>
    {
        private IntWritable result = new IntWritable();
        private static List<String> links;
        private String ss = "";
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int page_number = context.getConfiguration().getInt("page_number", 0);
                String page_rank = ((double)1/page_number) + "";

                String output = page_rank;
                for (final Text value : values) {
                    output += ":::" + value.toString();
                }
                outputValue.set(output);
                context.write(key, outputValue);
        }
    }

    public boolean run(final int page_number) throws Exception {
        //final String output = baseOutput + OUTPUT_PATH;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":::"); // set OUTPUT_SEPARATOR as separator
        //conf.set("stream.map.output.field.separator", "::");
        // instantiate job
        final Job job = new Job(conf, "Count");
        job.setJarByClass(Count.class);
        //job.setNumReduceTasks(5);

        // set mapper/reducer
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(input_file));
        FileOutputFormat.setOutputPath(job, new Path(output_file));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().setInt("page_number", page_number);

        return job.waitForCompletion(true);
    }

    public static String getTitle(String page){
        String title = "";
        int end_tag = 0;

        Pattern pattern_title = Pattern.compile("<title.*?>(.*?)<\\/title>");
        Matcher matcher_title = pattern_title.matcher(page);
        if (matcher_title.find()){
            title = matcher_title.group();
            end_tag = title.indexOf("</title>");
            title = title.substring(7, end_tag); //7 is the "<title>" tag length
            return title;
        }

        return null;
    }

    public static List<String> getLinks(String page){
        List<String> out_links = new LinkedList<>();
        String link = "";
        int end_tag = 0;

        Pattern pattern_links = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher matcher_links = pattern_links.matcher(page);
        while (matcher_links.find()){
            link = matcher_links.group();
            end_tag = link.indexOf("]]");
            link = link.substring(2, end_tag); //2 is the "[[" tag length

            out_links.add(link);
        }

        if(out_links.size() > 0)
           return out_links;

        return null;
    }
}

