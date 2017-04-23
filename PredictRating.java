package finalproject_try;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class PredictRating { 
	
public static class PredMapper extends Mapper<Object, Text, Text, Text>{
   
   //user0,customer1,sim1,movie0:rating0,movie1:rating1
   
   private Text outKey = new Text();
   private Text outValue = new Text();

   public void map(Object key, Text value, Context context
          ) throws IOException, InterruptedException {
	  String[] line = value.toString().split(",");
	  String user = line[0];
      float sim = Float.parseFloat(line[2]);
      String movie = "";
      float rating = 0;
      
      for(int i = 3; i < line.length; i++){
    	  String[] mr = line[i].split(":");
    	  movie = mr[0];
    	  rating = Float.parseFloat(mr[1]);
    	  outKey.set(user+":"+movie);
    	  outValue.set(String.valueOf(sim)+":"+String.valueOf(sim*rating));
    	  context.write(outKey, outValue);	
      }
   } 
}

public static class PredReducer extends Reducer<Text,Text,Text,Text> {
	
	//user:movie,[sim1:product1,sim2:product2,..]
	//user:movie,rating
	Text outValue = new Text();

	public void reduce(Text key, Iterable<Text> values,Context context
	                ) throws IOException, InterruptedException {
		
		float sum_sim = 0;
		float sum_product = 0;
		
		for(Text val:values){
			String[] line = val.toString().split(":");
			float sim = Float.parseFloat(line[0]);
			float product = Float.parseFloat(line[1]);
			sum_sim = sum_sim+sim;
			sum_product = sum_product+product;
		}
		
		outValue.set(key.toString()+","+String.valueOf(sum_product/sum_sim));
		context.write(null, outValue);
		
	}
}

public static class PredPartitioner extends HashPartitioner<Text, Text>{
	
	@Override
    public int getPartition(Text key, Text result, int numReduceTasks) {
		int num = key.toString().hashCode();
		return (num&Integer.MAX_VALUE)%numReduceTasks;
    }  
	
}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "predict rating");
	    job.setJarByClass(PredictRating.class);
	    
	    MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[5]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[6]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[7]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[8]),TextInputFormat.class,PredMapper.class);
//	    MultipleInputs.addInputPath(job,new Path(args[9]),TextInputFormat.class,PredMapper.class);
    
	    job.setPartitionerClass(PredPartitioner.class);
	    job.setNumReduceTasks(5);
	    job.setReducerClass(PredReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
