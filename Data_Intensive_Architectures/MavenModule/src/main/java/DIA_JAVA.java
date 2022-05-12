import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DIA_JAVA{

    public static class DeathMapper extends Mapper < Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString(); //Read each record
            String[] parts = record.split(" "); // Parse CSV file
            context.write(new Text(parts[0]), new Text("DEAT " + parts[1])); //Label Customers
        }
    }

    public static class FertilityMapper extends Mapper < Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString(); // Read each record
            String[] parts =record.split(" "); // Parse CSV File
            context.write(new Text (parts[0]), new Text ("FERT "+ parts[3])); // Label Transactions
        }
    }

//    public static class ContraceptiveMapper extends Mapper < Object, Text, Text, Text > {
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String record = value.toString(); // Read each record
//            String[] parts =record.split(" "); // Parse CSV File
//            context.write(new Text (parts[1]), new Text ("CONT "+ parts[3])); // Label Transactions
//        }
//    }

    public static class ReduceJoinReducer extends Reducer < Text, Text, Text, Text > {
        public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
            String name = "";
            double total = 0.0;
            int count = 0;
            /* Here is where the logic of the JOIN / reduction is laid out
                In this case this sections counts the number of customers and the number of transactions per customer along with their value */
            for (Text t : values) {
                String parts[] = t.toString().split(" ");
                if (parts[0].equals("FERT ")) {
                    count++; // count the number of fertilities
                    total += Float.parseFloat(parts[1]); // add up the fertility rates for every year

                } else if (parts[0].equals("DEAT ")) {
                    name = parts[1];
                }
                ; // count the number of customers
            }

            String str = String.format("%d %.2f", count, total);
            context.write(new Text(name), new Text(str));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side-Join");
        job.setJarByClass(DIA_JAVA.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DeathMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FertilityMapper.class);
//        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ContraceptiveMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

