import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FullInvertedIndex {

    public static class IndexMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            int position = 0;
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                output.collect(new Text(token.toLowerCase()),
                        new Text(fileName + ":" + position));
                position++;
            }
        }
    }
   public static class IndexReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {

            HashMap<String, List<Integer>> filePositionsMap = new HashMap<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                String[] fileAndPos = value.split(":");
                String fileName = fileAndPos[0];
                int position = Integer.parseInt(fileAndPos[1]);

                if (!filePositionsMap.containsKey(fileName)) {
                    filePositionsMap.put(fileName, new ArrayList<>());
                }
                filePositionsMap.get(fileName).add(position);
            }

            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, List<Integer>> entry : filePositionsMap.entrySet()) {
                result.append(entry.getKey()).append(":");
                List<Integer> positions = entry.getValue();
                for (int i = 0; i < positions.size(); i++) {
                    result.append(positions.get(i));
                    if (i < positions.size() - 1) {
                        result.append(",");
                    }
                }
                result.append(" ");
            }

            output.collect(key, new Text(result.toString().trim()));
        }
    }

    public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(FullInvertedIndex.class);
        conf.setJobName("fullInvertedIndexer");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(IndexMapper.class);
        //conf.setCombinerClass(IndexReducer.class);
        conf.setReducerClass(IndexReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

