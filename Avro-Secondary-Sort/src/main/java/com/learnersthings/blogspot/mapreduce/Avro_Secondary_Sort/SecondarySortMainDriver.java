package com.learnersthings.blogspot.mapreduce.Avro_Secondary_Sort;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Avro Secondary Sot Example
 *
 */
public class SecondarySortMainDriver extends Configured implements Tool {

	public static final String MAP_KEY_SCHEMA = "secondarysort.key.schema";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new SecondarySortMainDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//Get Input Arguments
		String input = args[0];// input file "/src/resources/input_data.avro"
		String output = args[1];// output path "/src/resources/output"
		String inputSchemaFile = args[2];// input schema File "/resources/schema.avsc"
		String mapKeySchemaFile = args[3];// Mapper Key Schema "/resources/map_key_schema.avsc"
		
		
		//Initialize MapReduce Job Conf
		JobConf conf = new JobConf(getConf(), SecondarySortMainDriver.class);
		conf.setJobName("Avro Secondary Sort ");
		
		//Set Input and Output Paths
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		//Get Input avro Schema From input Arguments
		Schema schema = new Schema.Parser().parse(new File(inputSchemaFile));
		//Get Mapper Key  avro Schema From input Arguments
		Schema mapKeySchema = new Schema.Parser().parse(new File(mapKeySchemaFile));
		
		//Side Data Distribution for use it in Mapper Class. 
		//FIXME you can get this key schema using AvroJob in mapper class no need to do Side Data Distribution 
		conf.set(MAP_KEY_SCHEMA, mapKeySchema.toString());
		
		//Set Input,MapOutput and Output Schema
		AvroJob.setInputSchema(conf, schema);
		AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(mapKeySchema, schema));
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), schema));
		
		//Set Mapper and Reducer  Class
		AvroJob.setMapperClass(conf, SecondarySortMapper.class);
		AvroJob.setReducerClass(conf, SecondarySortReducer.class);
		
		//Set Grouping and Partitioner class
		conf.setOutputValueGroupingComparator(GroupingKeyComparator.class);
		conf.setPartitionerClass(Part.class);
		JobClient.runJob(conf);
		return 0;
	}

	
	//Reducer Side Data Should be Groupoed Based on Id only not on noth Id and TimeStamp.
	//Input to this Class is Map_Key_SCHEMA
	public static class GroupingKeyComparator extends AvroKeyComparator<GenericRecord> {
		public int compare(AvroWrapper<GenericRecord> x, AvroWrapper<GenericRecord> y) {
			return String.valueOf(x.datum().get("Id")).compareTo(String.valueOf(y.datum().get("Id")));
		}
	}

	//Which Keys Should go to which Reducer is determines this Class.
	public static class Part implements Partitioner<AvroKey<GenericRecord>, AvroValue<GenericRecord>> {

		@Override
		public void configure(JobConf arg0) {
		}

		@Override
		public int getPartition(AvroKey<GenericRecord> key, AvroValue<GenericRecord> value, int numberOfPartitions) {
			return Math.abs(String.valueOf(key.datum().get("Id")).hashCode() % numberOfPartitions);
		}
	}

}
