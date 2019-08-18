package com.learnersthings.blogspot.mapreduce.Avro_Secondary_Sort;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;

	
public class SecondarySortReducer  extends AvroReducer<GenericRecord, GenericRecord, Pair<Integer, GenericRecord>>{
	@Override
	public void reduce(GenericRecord key, Iterable<GenericRecord> values,
			AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException {
		
		for (GenericRecord genericRecord : values) {
			collector.collect(new Pair<Integer, GenericRecord>(Integer.parseInt(key.get("Id").toString()),genericRecord));
		}
	}

}
