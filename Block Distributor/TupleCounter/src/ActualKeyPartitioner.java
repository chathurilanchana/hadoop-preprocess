import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class ActualKeyPartitioner implements Partitioner<Text, Text>
{
	HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
	Text newKey = new Text();
	HashMap<String, Integer> mappingList = new HashMap<String, Integer>();

	@Override
	public void configure(JobConf conf)
	{
		// TODO Auto-generated method stub
		String mappingStr = conf.get("map.input.nodes");
		System.out.println(mappingStr);
		if (mappingStr != null)
		{
			String[] pairs = mappingStr.split(";");
			for (int i = 0; i < pairs.length; i++)
			{
				String[] pair = pairs[i].split(":");
				mappingList.put(pair[0].toLowerCase(), Integer.parseInt(pair[1]));
			}
		}

	}

	@Override
	public int getPartition(Text key, Text value, int numReduceNodes)
	{
         System.out.println(numReduceNodes);
		if (mappingList.size() > 0 && mappingList.containsKey(key.toString().toLowerCase()))
		{
			System.out.println("partition key is "+key.toString()+" val is "+mappingList.get(key.toString().toLowerCase()));
           return mappingList.get(key.toString().toLowerCase());
		}
		else
		{
			System.out.println("Using same old partition");   
			try
			{
				return hashPartitioner.getPartition(key, value, numReduceNodes);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return (int) (Math.random() * numReduceNodes);
			}
		}
	}

}
