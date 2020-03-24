package com.feltap.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * @Autor FelTap23
 * 	This application performs the below query using KTable
 * 	
 * 	Sample JSON payload 
 *  {"id":"1111","name":"Luis","lastName":"Lopez","salary":500,"age":25}
 *  
 *  Where the id is the key
 *  
 *  SELECT  last_name, sum(salary)  AS total FROM person GROUP BY last_name
*/

@SpringBootApplication
@EnableBinding(QueryBinding.class)
public class QuerySample {
	
	public static void main(String[] args) {
		SpringApplication.run(QuerySample.class, args);
	}
	
	
	@StreamListener
	public void kafkaQuery(@Input(QueryBinding.INPUT)KTable<String, String> table){
		
		final ObjectMapper objectMapper = new ObjectMapper();
		
		table
			.mapValues(  (key,value) -> {
				try {
					return objectMapper.readTree(value);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
					return null;
				}
			})
			.filter( (key,value)  -> {
				return value != null && value.has("id")  && value.has("salary") && value.has("lastName");})
			.groupBy(  (key,value) -> new KeyValue<String, Integer>( value.get("lastName").asText()  , value.get("salary").asInt(0))  , Grouped.with(Serdes.String(),Serdes.Integer()))
			.aggregate( () -> 0 ,  (aggKey , newValue, aggValue) -> newValue + aggValue, (aggKey , newValue, aggValue) -> aggValue - newValue , Materialized.with(Serdes.String(), Serdes.Integer()))
			.toStream()
			.peek( (key,value) -> System.out.printf("%s->%d\n",key,value));
		
	}
}
