package com.learnersthings.blogspot.mapreduce.Avro_Secondary_Sort;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class SecondarySortMapper extends AvroMapper<GenericRecord, Pair<GenericRecord, GenericRecord>> {

	private Schema keySchema;

	@Override
	public void configure(JobConf jobConf) {
		keySchema = new Schema.Parser().parse(jobConf.get(SecondarySortMainDriver.MAP_KEY_SCHEMA));
	}

	@Override
	public void map(GenericRecord datum, AvroCollector<Pair<GenericRecord, GenericRecord>> collector, Reporter reporter)
			throws IOException {
		GenericRecord record = new GenericData.Record(keySchema);
		record.put("Id", Integer.parseInt(datum.get("Id").toString()));
		record.put("TimeStamp", Long.parseLong(datum.get("TimeStamp").toString()));
		collector.collect(new Pair<GenericRecord, GenericRecord>(record, datum));
	}
}
