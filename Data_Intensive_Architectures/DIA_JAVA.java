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

public class DIA_JAVA {
 public static class FertMapper extends Mapper < Object, Text, Text, Text > {
  public void map(Object key, Text value, Context context)
  throws IOException,
  InterruptedException {
   String record = value.toString(); //Read each record
   String[] parts = record.split(","); // Parse CSV file 
   context.write(new Text(parts[1]), new Text("FERT:" + parts[0] + ":" + parts[3])); //country code, country name, fertility rates.
  }
 }

 public static class DeathMapper extends Mapper < Object, Text, Text, Text > {
  public void map(Object key, Text value, Context context)
  throws IOException,
  InterruptedException {
   String record = value.toString(); // Read each record
   String[] parts = record.split(","); // Parse CSV File
   context.write(new Text(parts[0]), new Text("DEATH:" + parts[2])); //country code, new COVID cases
  }
 }

 public static class ReduceJoinReducer extends Reducer < Text, Text, Text, Text > {
  public void reduce(Text key, Iterable < Text > values, Context context)
  throws IOException,
  InterruptedException {
   String country = "";
   double total_death = 0.0;
   double avg_fert = 0.0;
   int count = 0;

   for (Text t: values) {
    String parts[] = t.toString().split(":");
    if (parts[0].equals("DEATH")) {
     total_death += Float.parseFloat(parts[1]); // sum the number of annual deaths
    } 

    else if (parts[0].equals("FERT")) {
     count++; //count the number of records
     country = parts[1]; 
     avg_fert += Float.parseFloat(parts[2]); //sum the fertility rate in prep to divide by the number of records
    }

   }
   String str = String.format("%.2f\t%.2f", total_death, (avg_fert/count));
   context.write(new Text(country), new Text(str));
  }
 }

 public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = new Job(conf, "Reduce-side Join");
  job.setJarByClass(DIA_JAVA.class);
  job.setReducerClass(ReduceJoinReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FertMapper.class);
  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeathMapper.class);
  Path outputPath = new Path(args[2]);
  FileOutputFormat.setOutputPath(job, outputPath);
  outputPath.getFileSystem(conf).delete(outputPath);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}

