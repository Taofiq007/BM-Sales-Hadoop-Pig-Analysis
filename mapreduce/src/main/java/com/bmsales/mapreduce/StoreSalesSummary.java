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

public class StoreSalesSummary {
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("date,")) return;
            String[] f = line.split(",", -1);
            if (f.length < 9) return;
            try {
                outKey.set(f[1]); outValue.set(Double.parseDouble(f[6])); context.write(outKey, outValue);
            } catch (NumberFormatException e) {
                // Skip malformed numeric rows.
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double total=0.0; for (DoubleWritable v: values) total += v.get(); context.write(key, new DoubleWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StoreSalesSummary <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BM Sales - Store Sales Summary");
        job.setJarByClass(StoreSalesSummary.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
