package org.tkorostelev.meta_point02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class MetaPoint02 extends Configured implements Tool{

    private void calculateAndSetPositions(Configuration conf, String all_columns, String unpivot_columns) {

        // Calculate positions
        String[] all_columns_split = all_columns.split(",");
        List<String> unpivot_columns_split = Arrays.asList(unpivot_columns.split(","));

        StringBuilder key_columns_positions = new StringBuilder();
        StringBuilder unpivot_columns_positions = new StringBuilder();

        for (int i=0;i < all_columns_split.length;i++) {
            if (unpivot_columns_split.contains(all_columns_split[i])) {
                unpivot_columns_positions.append(Integer.toString(i));
                unpivot_columns_positions.append(",");
            }
            else {
                key_columns_positions.append(Integer.toString(i));
                key_columns_positions.append(",");
            }
        }

        // Write positions to parameters in configuration
        conf.set("key_columns_positions", cutLastSymbol(key_columns_positions.toString()));
        conf.set("unpivot_columns_positions", cutLastSymbol(unpivot_columns_positions.toString()));
    }

    // I don't like Java because I must write this
    private String cutLastSymbol(String in) {
        if (in == null || in.length() == 0) {
            return in;
        }
        return in.substring(0, in.length()-1);
    }

    private String getFirstLine(Configuration conf, String path)
            throws IOException {

        FileSystem fs = FileSystem.get(conf);
        BufferedReader file = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));

        String output = file.readLine();

        file.close();
        fs.close();

        return output;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Point02 <input_path> \"col1,col2,col3\" <output_path>");
            System.exit(-1);
        }

        final String inputPath = args[0];
        final String unpivot_columns = args[1];
        final String outputPath = args[2];

        final Configuration conf = getConf();

        String first_line;
        try {
            first_line = getFirstLine(conf, inputPath);
        }
        catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
        calculateAndSetPositions(conf, first_line.replaceAll("(\\r|\\n)", ""), unpivot_columns);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MetaPoint02.class);
        job.setJobName("Meta_Point02");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(MetaPoint02Mapper.class);

        // We need neither reducer, nor sorting
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MetaPoint02(), args);
        System.exit(exitCode);
    }
}
