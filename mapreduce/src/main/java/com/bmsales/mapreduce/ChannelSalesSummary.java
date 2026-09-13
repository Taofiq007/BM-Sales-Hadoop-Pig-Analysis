package com.bmsales.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ChannelSalesSummary {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("date,")) return;
            String[] f = line.split(",", -1);
            if (f.length < 9) return;
            try {
                outKey.set(f[7]); // channel
                outValue.set("1," + Double.parseDouble(f[6]));
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
                // Skip malformed records.
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long count = 0L;
            double total = 0.0;
            for (Text t : values) {
                String[] p = t.toString().split(",", 2);
                count += Long.parseLong(p[0]);
                total += Double.parseDouble(p[1]);
            }
            context.write(key, new Text(count + "\t" + String.format("%.2f", total)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ChannelSalesSummary <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BM Sales - Channel Sales Summary");
        job.setJarByClass(ChannelSalesSummary.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
