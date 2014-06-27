import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class Reduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

		 JobConf conf;
	 String outputDir="";
	 private MultipleOutputs mos;
	 public void configure(JobConf conf) {
		 outputDir=conf.get("outputloc");
		 this.conf=conf;
		 mos = new MultipleOutputs(conf);
		 }

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		 Text t=new Text();
		  Text tuples=new Text();
		int count=0;//to keep the count of appearance of tuples
		 Set<String> set = new HashSet<String>();//To get the distinct tuple combination. Always this is 2
	     System.out.println("Reducer Started");
		 while (values.hasNext()) {
			 tuples=values.next();
		System.out.println(tuples.toString());
		for(int i=0;i<3;i++){
		mos.getCollector("col"+i, "file"+i, reporter).collect(
				NullWritable.get(), new Text("data"+i));
		}
		}
		
			mos.getCollector("col"+0, "file"+1, reporter).collect(
					NullWritable.get(), new Text("data"+1));
	
		
	}
	 private void CreateFile(String line) throws Exception {
			// TODO Auto-generated method stub

			    // Create a new file and write data to it.
			  //  FSDataOutputStream out = fileSystem.create(path);
			    byte[] dataToCompress=line.getBytes();
			   // byte[] dataToCompress ="This is a test".getBytes("UTF-8");
			   
			    
	 FileSystem fileSystem = FileSystem.get(conf);
	   
	    // Check if the file already exists
	    Path path = new Path(outputDir+"/file-0.txt");
	    if (fileSystem.exists(path)) {
	        System.out.println("File " + path + " already exists");
	        return;
	    }
				try {
	                //BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(path,true)));
					DataOutputStream br=new DataOutputStream(fileSystem.create(path,true));
					br.write(dataToCompress);
				    br.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			   // out.close();
			   // fileSystem.close();	
		} 
	 public void close() throws IOException
		{
			mos.close();
		}

}
