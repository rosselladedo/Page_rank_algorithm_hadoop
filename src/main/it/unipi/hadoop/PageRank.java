package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Integer;

public class PageRank {

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int pagenumber = 0;
        double alpha = 0.15;

        if(otherArgs.length != 3) {
            System.err.println("Usage: PageRank <input> <base output> <# of iterations> <# of reducers> <random jump$
            System.exit(1);
        }

        final String input = otherArgs[0];
        final String output = otherArgs[1];
        final int iterations = Integer.parseInt(otherArgs[2]);

        String output_count = output + "/count";
        String output_rank = output + "/rank";
        String output_sort = output + "/sort";

        Count c = new Count(input, output_count);
        pagenumber = c.pageNumber();
        c.run(pagenumber);

        System.out.println("FASE DI COUNT COMPLETATA!");

        String new_input = output_count + "/part-r-00000";

        RankingCalculation r = new RankingCalculation(new_input, output_rank);
        r.run(pagenumber, alpha, iterations);

        System.out.println("FASE DI RANKING COMPLETATA!");

        if (iterations == 1)
            new_input = output_rank + "/part-r-00000";
        else
            new_input = output_rank + (iterations-1) + "/part-r-00000";
        SortPage s = new SortPage(new_input, output_sort);
        s.run();

        System.out.println("FASE DI SORT COMPLETATA!");
    }
}

