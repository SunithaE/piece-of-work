package com.ceb.es_hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;

public class ElasticSinkHadoopSourceJobNewApi {


	public static TransportClient client;
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
	public static void main(String args[]){

		System.out.println("-----------START-----------"); 
		try
		{
			Builder settingsBuilder = ImmutableSettings.settingsBuilder()
					.put("client.transport.ping_timeout", "6s");
			client = new TransportClient(settingsBuilder)
			.addTransportAddress(new InetSocketTransportAddress("10.78.130.74", 9300));

			System.out.println("Connection and indexing....");

			System.out.println("Search index and query....");
			long epochTime = aggregateQuery(client);
			System.out.println("The date is ... " +epochTime);
			Date date = new Date(epochTime);
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			String formattedDate = sdf.format(date);
			System.out.println("formatted date is ... " +formattedDate);

			Properties prop = new Properties();

			prop.load(new FileInputStream(new 
					File("/home/cloudera/workspace/ElasticSearchNativeIntegrationToHadoop/src/config.properties")));

			ArrayList<String> config = new ArrayList<>();
			config.add(prop.getProperty("ES_node"));
			config.add(prop.getProperty("ES_resource"));
			config.add(prop.getProperty("ES_input_json"));
			config.add(prop.getProperty("ES_index_auto_create"));
			config.add(prop.getProperty("ES_write_operation"));
			config.add(prop.getProperty("ES_mapping_id"));

			
			Configuration conf1 = new Configuration();
			conf1.setBoolean("mapred.map.tasks.speculative.execution", false);

			System.out.println("Setting es parameters....");
			conf1.set("es.nodes",config.get(0));
			conf1.set("es.resource", config.get(1));
			conf1.set("es.input.json", config.get(2));
			conf1.set("es.index.auto.create", config.get(3));
			conf1.set("es.write.operation", config.get(4));
			conf1.set("es.mapping.id", config.get(5));
			
			Job job = new Job(conf1);
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
			
			job.setOutputFormatClass(EsOutputFormat.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(ElasticSinkHadoopSourceMapperNewApi.class);
		//	job.setMapperClass(ElasticSinkHadoopSourceMapper.class);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			System.out.println("Exit");
		}
		catch(Exception e)
		{
			System.out.println("Exception..." +e);
		}
	}

	private static long aggregateQuery(TransportClient client2)
	{
		long value = 0;
		try
		{
			SearchResponse res = client2.prepareSearch("dec8").setTypes("job_post").addAggregation
					(AggregationBuilders.max("agg").field("increment_date")).execute().actionGet();

			System.out.println("res is ... " +res.getAggregations().get("agg").toString());
			Max agg = res.getAggregations().get("agg");
			value = (long) agg.getValue();
			System.out.println("The result of the aggregate query is ... " +agg.getValue());	
			//return value;
		}
		catch(Exception e)
		{
			System.out.println("The aggregation query result is .... "+e);
		}
		return value;
	}
}
