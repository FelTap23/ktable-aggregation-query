package com.feltap.streams.customserde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonSerde  extends WrapperSerde<JsonNode>{

	public JsonSerde() {
		super( new JsonSerializer(), new JsonDeserializer());
	}
	
	public JsonSerde(Serializer<JsonNode> serializer, Deserializer<JsonNode> deserializer) {
		super( serializer, deserializer);
	}
	
}