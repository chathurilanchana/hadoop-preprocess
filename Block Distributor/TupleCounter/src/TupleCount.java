import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TupleCount extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new TupleCount(), args);
		System.exit(res);

		// job.waitForCompletion(true);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Output location is "+args[2]);
		JobConf conf = new JobConf(TupleCount.class);
		conf.setJobName("tuplecount");
		conf.setPartitionerClass(ActualKeyPartitioner.class);
		conf.setNumReduceTasks(3); 
		conf.setOutputKeyClass(Text.class);//Both map and reduce map has <Text,Text> format
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(MultipleTextOutputFormat.class);
		 TextOutputFormat.setCompressOutput(conf, true);
		 TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class); 
		conf.set("map.input.nodes", "thera:0;melos:1;lesbos:2");
		conf.setIfUnset("columnCount" , args[0]);
		conf.setIfUnset("outputloc", args[2]);
		conf.setBoolean("dfs.blocks.rowEnabled", true);
		FileInputFormat.setInputPaths(conf, new Path(args[1]));//input file path
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));//output file pth

		for(int i=0;i<3;i++){
			 MultipleOutputs.addMultiNamedOutput(conf, "col"+i, TextOutputFormat.class, Text.class, Text.class);
			}
			
		
		JobClient.runJob(conf);
		
		//print file locations
		System.out.println("job completed");

		
		PrintWriter writer = new PrintWriter("/usr/local/hadoop/sample.txt", "UTF-8");
	
	
		
		String filename = args[2];
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus[] status = fileSystem.listStatus(new Path(filename));
		if (status.length > 0 )
		{
			for (int i = 0; i < status.length; i++)
			{
				if(!status[i].isDir()){
				System.out.println(status[i].getPath());
				writer.println(status[i].getPath());
				BlockLocation[] loc = fileSystem.getFileBlockLocations(
						status[i], 0, status[i].getLen());
				for (int j = 0; j < loc.length; j++)
				{
					System.out.println("----" + loc[j]);
					writer.println("----" + loc[j]);
				}
				}
			}
		}
		else
		{
			System.out.println("No output file found");
		}
		writer.close();
		return 0;
	}

}
