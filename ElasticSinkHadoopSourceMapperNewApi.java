package com.ceb.es_hadoop;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ElasticSinkHadoopSourceMapperNewApi extends org.apache.hadoop.mapreduce.Mapper
{
	public void map(Object key, Object value, OutputCollector<NullWritable,Text> output, Reporter report){

		System.out.println("Mapper class...");
		try
		{
			String source = value.toString();
			System.out.println("The complete string is .... " +source);
			Text jsonDoc = new Text(source);

			System.out.println("The json doc is .... " +jsonDoc); 
			output.collect(NullWritable.get(), jsonDoc);
			//}
		}
		catch(Exception e)
		{
			System.out.println("Exception in mapper class.... " +e);
		}
	}
}