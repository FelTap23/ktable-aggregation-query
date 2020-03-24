package com.feltap.streams;

import org.apache.kafka.streams.kstream.KTable;
import org.springframework.cloud.stream.annotation.Input;

public interface QueryBinding {
	String INPUT = "input";
	@Input(INPUT)
	public KTable<?, ?> process();
}
