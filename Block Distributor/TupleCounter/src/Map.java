import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

public class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text>
{
	private final IntWritable one = new IntWritable(1);
	Text word = new Text();
	Text sort = new Text();
	String fileName = null;

	public void configure(JobConf job)//read the config. If the node does not exist add it with unique key
	{
		System.out.println("configuring");
		fileName = job.get("map.input.file");
		int currentNodeId;
		//StringBuilder nodeStr = new StringBuilder();
		//String mappingStr = job.get("map.input.nodes");
		/*if (mappingStr == null)
		{
			nodeStr.append(getHostName()).append(":").append(0);
			job.set("map.input.nodes", nodeStr.toString());
		}
		else
		{ // Add only if the node does not extst;
			HashMap<String, Integer> nodeIdMapping = getNodeIdMapping(mappingStr);
			if (!nodeIdMapping.containsKey(getHostName()))
			{
				currentNodeId = getNodeIdToAdd(nodeIdMapping);
				nodeStr.append(mappingStr).append(";").append(getHostName())
						.append(":").append(currentNodeId);
				job.set("map.input.nodes", nodeStr.toString());
			}

		}  */

	}

	private int getNodeIdToAdd(HashMap<String, Integer> nodeIdMapping)
	{
		// TODO Auto-generated method stub
		return Collections.max(nodeIdMapping.values())+1;
	}

	private HashMap<String, Integer> getNodeIdMapping(String mappingStr)
	{
		HashMap<String, Integer> nodeMap = new HashMap<String, Integer>();
		int nodeId;
		String[] pairs = mappingStr.split(";");
		for (int i = 0; i < pairs.length; i++)
		{
			String[] pair = pairs[i].split(":");
			nodeMap.put(pair[0], Integer.parseInt(pair[1]));
		}
		return nodeMap;
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException
	{
		// TODO Auto-generated method stub
		String keyVal = getHostName();
		sort.set(keyVal.toString());
		word.set(value.toString());
		outputCollector.collect(sort, word);// Output has a <Text,Text> format
	}

	private String getHostName()
	{
		try
		{
			return java.net.InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e)
		{
			// TODO log exception
			return null;
		}
	}

}
