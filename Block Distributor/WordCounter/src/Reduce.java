import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class Reduce extends MapReduceBase implements
		Reducer<CompositeKey, Writable, Text, Text>
{
	private MultipleOutputs mos;
	JobConf conf;
	String outputDir = "";
	int blockId;
	int maxBlockSize=134217728;// 128MB u can even use half of this as well.
									// block size.Should read from config
	
	//int maxBlockSize=100000;
	HashMap<Integer, StringBuilder> currentFileList;
	HashMap<Integer, Integer> sizeList;
	HashMap<Integer, String> fileMapping;
	Boolean isNewBlock = true;
	Boolean isFirstTime = true;

	public void configure(JobConf conf)
	{
		outputDir = conf.get("outputloc");
		
		String confSize=conf.get("reduce.block.size");
		if(confSize!=null)
		{
		maxBlockSize=Integer.parseInt(confSize);
		}
		
		mos = new MultipleOutputs(conf);
		this.conf = conf;
	}

	@Override
	public void reduce(CompositeKey key, Iterator<Writable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{

		System.out.println("reducer started");
		System.out.println("Block size is "+maxBlockSize );
		// TODO Auto-generated method stub

		Boolean isStartRecord = true;
		while (values.hasNext())
		{
			RowData row = (RowData) values.next();
			if (isStartRecord)
			{
				initDataStructurs(row);
			}
			if (row != null)
			{
				if (isNewBlock(row) || isStartRecord)
				{
					isStartRecord = false;
					clearDataStructure(row);
					addSingleRow(row, reporter,key.getHost());
				}
				else
				{
					addSingleRow(row, reporter,key.getHost());
				}
			}
		}	
		writeSerializedOutput();
	HashMap<String, Employee> emp=readSerializedOutput();
	if(emp==null){
		System.out.println("employee null");
	}
	else{
		System.out.println(emp.get("gayya").address);
	}
	}

	private HashMap<String,Employee> readSerializedOutput() {
		// TODO Auto-generated method stub
		try {
		List<Employee>emp=new ArrayList<Employee>();
		Path pt=new Path(outputDir+"/file-0.txt");
		FileSystem fs = FileSystem.get(conf);
		ObjectInputStream br=new ObjectInputStream(fs.open(pt));
	    HashMap<String, Employee> e=(HashMap<String,Employee>) br.readObject();
        return e;
		
		} 
		catch(Exception ex){
			ex.printStackTrace();
			return null;
		}
	}

	private void initDataStructurs(RowData rowData)
	{
		// TODO Auto-generated method stub
		blockId = 0;
		currentFileList = new HashMap<Integer, StringBuilder>();
		sizeList = new HashMap<Integer, Integer>();
	}

	private void clearDataStructure(RowData row)
	{
		blockId++;
		for (int i = 0; i < row.getColumns().size(); i++)
		{
			// currentFileList.get(i).setLength(0);
			sizeList.put(i, 0);
		}
	}

	private void addSingleRow(RowData data, Reporter reporter,String hostName)
			throws IOException
	{
		 String	seqName = hostName+ "ZblockZ" + blockId;
		for (int i = 0; i < data.getColumns().size(); i++)
		{
			mos.getCollector("col"+i, seqName, reporter).collect(
					NullWritable.get(), new Text(data.getColumns().get(i)));

			int oldSize = sizeList.get(i);
			try
			{
				int updatedSize = oldSize
						+ data.getColumns().get(i).getBytes("UTF-8").length;
				sizeList.put(i, updatedSize);
			}
			catch (UnsupportedEncodingException e)
			{
				// TODO Auto-generated catch block
				System.out.println("Exception Thrown while appending a row");
				e.printStackTrace();
			}
		}
	}

	private Boolean isNewBlock(RowData r)
	{
		for (int i = 0; i < r.getColumns().size(); i++)
		{

			int blockLength;
			try
			{
				if (sizeList.size() != 0)
					blockLength = sizeList.get(i)
							+ r.getColumns().get(i).getBytes("UTF-8").length;
				else
				{
					blockLength = r.getColumns().get(i).getBytes("UTF-8").length;
				}
				if (blockLength > maxBlockSize)
				{
					// save Existing block
					// CreatePreviousFile();
					return true;
				}
			}
			catch (UnsupportedEncodingException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.out.println("Error occured while getting the UTF-8 length for field"
								+ i);
			}

		}

		return false;
	}


	public void close() throws IOException
	{
		mos.close();
	}
	
	public void writeSerializedOutput(){
		try {
		 FileSystem fileSystem = FileSystem.get(conf);
		   
		    // Check if the file already exists
		 HashMap<String,Employee> hm=new HashMap<String, Employee>();
		 
		 Employee e=new Employee();
		 e.address="no45";
		 e.name="chathu";
		 e.number=11111;
		 e.SSN=3;
		 Employee e2=new Employee();
		 e2.address="no45ee";
		 e2.name="chathuee";
		 e2.number=111114;
		 e2.SSN=34;
		 hm.put("gayya", e);
		    Path path = new Path(outputDir+"/file-0.txt");
		    if (fileSystem.exists(path)) {
		        System.out.println("File " + path + " already exists");
		        return;
		    }
					
		                //BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(path,true)));
						ObjectOutputStream br=new ObjectOutputStream(fileSystem.create(path,true));
						br.writeObject(hm);
						//br.writeObject(e2);
					    br.close();
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
	
	}

}
