package com.learning.kafkastreams;

import com.learning.kafkastreams.proc.WordCountProcessor;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class KafkaStreamsApplicationTests {

	@Autowired
	WordCountProcessor wordCountProcessor;

	@Test
	void givenInputMessages_whenProcessed_thenWordCountIsProduced() {
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		wordCountProcessor.buildPipeline(streamsBuilder);
		Topology topology = streamsBuilder.build();

		try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
			TestInputTopic<String, String> inputTopic = topologyTestDriver
					.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());

			TestOutputTopic<String, Long> outputTopic = topologyTestDriver
					.createOutputTopic("output-topic", new StringDeserializer(), new LongDeserializer());

			inputTopic.pipeInput("key", "hello world");
			inputTopic.pipeInput("key2", "hello");

			assertThat(outputTopic.readKeyValuesToList())
					.containsExactly(
							KeyValue.pair("hello", 1L),
							KeyValue.pair("world", 1L),
							KeyValue.pair("hello", 2L)
					);
		}
	}

}
