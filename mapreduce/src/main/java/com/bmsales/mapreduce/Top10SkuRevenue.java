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
import java.util.Comparator;
import java.util.PriorityQueue;

public class Top10SkuRevenue {
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("date,")) return;
            String[] f = line.split(",", -1);
            if (f.length < 9) return;
            try {
                outKey.set(f[2]);                  // sku_id
                outValue.set(Double.parseDouble(f[6])); // total_value
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
                // Skip malformed records.
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private PriorityQueue<SkuTotal> top10;

        protected void setup(Context context) {
            top10 = new PriorityQueue<>(10, Comparator.comparingDouble(x -> x.total));
        }

        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
            double total = 0.0;
            for (DoubleWritable v : values) total += v.get();
            SkuTotal item = new SkuTotal(key.toString(), total);
            if (top10.size() < 10) top10.offer(item);
            else if (total > top10.peek().total) {
                top10.poll();
                top10.offer(item);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            SkuTotal[] arr = top10.toArray(new SkuTotal[0]);
            java.util.Arrays.sort(arr, (a,b) -> Double.compare(b.total, a.total));
            for (SkuTotal x : arr) {
                context.write(new Text(x.sku), new DoubleWritable(x.total));
            }
        }
    }

    static class SkuTotal {
        String sku; double total;
        SkuTotal(String sku, double total) { this.sku = sku; this.total = total; }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Top10SkuRevenue <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 1);
        Job job = Job.getInstance(conf, "BM Sales - Top 10 SKU Revenue");
        job.setJarByClass(Top10SkuRevenue.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
