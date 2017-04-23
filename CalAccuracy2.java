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


public class CalAccuracy2 {
	
	//cnt,error
	public static class CalMapper extends Mapper<Object, Text, Text, Text>{
		
		Text outkey = new Text();
		Text outvalue = new Text();
		
		int sum_correct = 0;
		int sum_wrong = 0;
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
				   String[] line=value.toString().split(","); 
				   int correct = Integer.parseInt(line[0]);
				   int wrong = Math.round(Float.parseFloat(line[1]));
				   //int wrong = Integer.parseInt(line[1]);
				   sum_correct = sum_correct+correct;
				   sum_wrong = sum_wrong+wrong;
		}
		
		public void cleanup (Context context) throws IOException,InterruptedException {
    		outkey.set("0");
    		outvalue.set(String.valueOf(sum_correct)+","+String.valueOf(sum_wrong));
    		context.write(outkey,outvalue);
    	}
	}
	
	 public static class CalReducer extends Reducer<Text,Text,Text,Text> {
	    	
	    	//null,[sum_cnt,sum_error]
	    	//accuracy
	    	
		    Text outkey = new Text();
	    	Text outvalue = new Text();
	    	
	    	public void reduce(Text key, Iterable<Text> values,Context context
	    	                ) throws IOException, InterruptedException {
	    		int total_correct = 0;
		    	int total_wrong = 0;
	    			    		
	    		for(Text val : values){
	    			String[] line = val.toString().split(",");
	    			int sum_correct = Integer.parseInt(line[0]);
	    			int sum_wrong = Integer.parseInt(line[1]);
	    			total_correct = total_correct+sum_correct;
	    			total_wrong = total_wrong+sum_wrong;
	    		}
	    		outkey.set(String.valueOf(total_correct));
	    		outvalue.set(String.valueOf(total_wrong));
	    		context.write(outkey, outvalue);
	    	}
	    	
	    }
	
   public static class CalPartitioner extends HashPartitioner<Text, Text>{
    	
    	@Override
        public int getPartition(Text key, Text result, int numReduceTasks) {
    		return 0;
        }  
    	
    }
   
   public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "accuracy");
	    job.setJarByClass(CalAccuracy2.class);
	    MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,CalMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,CalMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,CalMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,CalMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,CalMapper.class);
	    
	    job.setPartitionerClass(CalPartitioner.class);
	    job.setNumReduceTasks(1);
	    job.setReducerClass(CalReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[5]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
