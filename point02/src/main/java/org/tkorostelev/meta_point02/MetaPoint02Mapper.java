package org.tkorostelev.meta_point02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.Stream;

public class MetaPoint02Mapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    private int[] key_columns_positions;
    private int[] unpivot_columns_positions;
    private Logger logger = Logger.getLogger(MetaPoint02Mapper.class);

    @Override
    public void setup(Context context)
            throws IOException {

        String key_columns_positions_line = context.getConfiguration().get("key_columns_positions");
        String unpivot_columns_positions_line = context.getConfiguration().get("unpivot_columns_positions");

        key_columns_positions =
                Stream.of(key_columns_positions_line.split(",")).mapToInt(Integer::parseInt).toArray();
        unpivot_columns_positions =
                Stream.of(unpivot_columns_positions_line.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    private String getKeyColumns(String[] line) {

        StringBuilder output = new StringBuilder();

        for(int position : key_columns_positions) {
            output.append(line[position]);
            output.append(",");
        }

        return output.toString();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Skip first line with columns definition
        if (key.get() == 0) {
            return;
        }

        String[] line_split = value.toString().split(",");

        String key_columns = getKeyColumns(line_split);

        for(int position : unpivot_columns_positions) {
            StringBuilder output = new StringBuilder(key_columns);
            output.append(line_split[position]);
            context.write(key, new Text(output.toString()));
        }

    }

}
