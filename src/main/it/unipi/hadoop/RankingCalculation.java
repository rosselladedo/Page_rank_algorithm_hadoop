package it.unipi.hadoop;

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

public class RankingCalculation {

    private String input_file = "";
    private String output_file = "";

    public RankingCalculation(String input, String output){
        input_file = input;
        output_file = output;
    }

    public static class RankingCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, Interru$
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while(itr.hasMoreTokens()){
                String token = itr.nextToken();
                String token_part [] = token.split(":::");
                String title = token_part[0];
                String page_rank = "page_rank:" + (double)Double.parseDouble(token_part[1])/(token_part.length - 2);$
                String links = "";

                for (int i = 2; i < token_part.length; i++){
                    if (i < (token_part.length - 1))
                        links = links + token_part[i] + ":::";
                    else
                        links = links + token_part[i];
                    context.write(new Text(token_part[i]), new Text(page_rank));
                }

                context.write(new Text(title), new Text(links));
            }
        }
    }

    public static class RankingCalculationReducer extends Reducer<Text, Text, Text, Text> {

        Double page_rank = 0.0;
        String links = "";
        String output = "";

        public void reduce(final Text key, final Iterable<Text> value, final Context context) throws IOException, In$
            int page_number = context.getConfiguration().getInt("page_number", 0);
            double alpha = context.getConfiguration().getDouble("alpha", 0);

            String title = key.toString();

            for (Text s : value){
                String v = s.toString();
                if (v.contains("page_rank:"))
                    page_rank = page_rank + Double.parseDouble(v.substring(10, v.length()));
                else
                    links = v;
            }

            page_rank = alpha*((double)1/page_number) + (1 - alpha)*page_rank;

            if(links.equals(""))
                output = page_rank + "";
            else
                output = page_rank + ":::" + links;

            context.write(key, new Text(output));
        }
    }

    public boolean run(final int page_number, final double alpha, final int iterations) throws Exception{
        boolean result = true;
        String new_output_file = output_file;

        for (int i = 1; i <= iterations; i++){

            final Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ":::"); // set OUTPUT_SEPARATOR as separator
            conf.set("stream.map.output.field.separator", ":::");
            // instantiate job
            final Job job = new Job(conf, "RankingCalculation");
            job.setJarByClass(RankingCalculation.class);
            //job.setNumReduceTasks(5);

            // set mapper/reducer
            job.setMapperClass(RankingCalculationMapper.class);
            job.setReducerClass(RankingCalculationReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // define reducer's output key-value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // define I/O
            FileInputFormat.addInputPath(job, new Path(input_file));
            FileOutputFormat.setOutputPath(job, new Path(new_output_file));

            // define input/output format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.getConfiguration().setInt("page_number", page_number);
            job.getConfiguration().setDouble("alpha", alpha);

            input_file = new_output_file + "/part-r-00000";
            new_output_file = output_file + i;
            System.out.println("new_output_file: " + new_output_file);
            System.out.println("input_file: " + input_file);

            job.waitForCompletion(true);

            if (!job.isSuccessful()){
                throw new RuntimeException("Job failed : " + job);
            }

            System.out.println("JOB COMPLETATO!");
        }

        return result;

    }
}
