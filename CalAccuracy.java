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


public class CalAccuracy {
	
	//cnt,error
	public static class CalMapper extends Mapper<Object, Text, Text, Text>{
		
		Text outkey = new Text();
		Text outvalue = new Text();
		
		int sum_cnt = 0;
		float sum_error = 0;
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
				   String[] line=value.toString().split(","); 
				   int cnt = Integer.parseInt(line[0]);
				   float error = Float.parseFloat(line[1]);
				   sum_cnt = sum_cnt+cnt;
				   sum_error = sum_error+error;
		}
		
		public void cleanup (Context context) throws IOException,InterruptedException {
    		outvalue.set(String.valueOf(sum_cnt)+","+String.valueOf(sum_error));
    		outkey.set("0");
    		context.write(outkey,outvalue);
    	}
	}
	
	 public static class CalReducer extends Reducer<Text,Text,Text,Text> {
	    	
	    	//null,[sum_cnt,sum_error]
	    	//accuracy
	    	
	    	Text outvalue = new Text();
	    	
	    	public void reduce(Text key, Iterable<Text> values,Context context
	    	                ) throws IOException, InterruptedException {
	    		int total_cnt = 0;
		    	float total_error = 0;
	    			    		
	    		for(Text val : values){
	    			String[] line = val.toString().split(",");
	    			int sum_cnt = Integer.parseInt(line[0]);
	    			float sum_error = Float.parseFloat(line[1]);
	    			total_cnt = total_cnt+sum_cnt;
	    			total_error = total_error+sum_error;
	    		}
	    		float accuracy = total_error/total_cnt;
	    		outvalue.set(String.valueOf(accuracy));
	    		context.write(null, outvalue);
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
	    job.setJarByClass(CalAccuracy.class);
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
