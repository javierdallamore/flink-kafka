package com.gnandoo

import java.util.Properties
import java.time.ZoneId;
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.watermark._

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import scala.util.parsing.json.JSONObject

case class Accumulator(from: Long, to: Long, username: String, var sum: Double, var count: Int)

class Aggregate extends AggregateFunction[(String, Double), Accumulator, Accumulator] {

  override def createAccumulator(): Accumulator = {
	return Accumulator(0L, 0L, "", 0.0, 0)
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
	a.sum += b.sum
	a.count += b.count
	return a
  }

  override def add(value: (String, Double), acc: Accumulator): Accumulator = {
	acc.sum += value._2
	acc.count += 1
	acc
  }

  override def getResult(acc: Accumulator): Accumulator = {
	return acc
  }
}

object KafkaStreamCount {
  val fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
	//env.enableCheckpointing(2000);
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

	val properties = new Properties()
	properties.setProperty("bootstrap.servers", "localhost:39092")
	// only required for Kafka 0.8
	properties.setProperty("zookeeper.connect", "localhost:32181")
	properties.setProperty("group.id", "test")
	val kafkaConsumer = new FlinkKafkaConsumer011[ObjectNode]("users",new JSONKeyValueDeserializationSchema(true), properties)
	val stream = env
	  .addSource(kafkaConsumer)
	  // Funciona en orden
	  .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[ObjectNode] {
		override def extractTimestamp(element: ObjectNode, prevElementTimestamp: Long): Long = {
		  element.get("value").get("ts").asLong
		}

		override def getCurrentWatermark(): Watermark = { 
			// permitir 2hs de demora
		  new Watermark(System.currentTimeMillis - (1000 * 60 * 60 * 2))
		}
	  })

	stream
	  .map(x => {
	  	val item = x.get("value")
		val username = item.get("username").asText()
		val value = item.get("val").asInt().toDouble
		(username, value)
	  })
	  .keyBy(x => x._1)
	  .timeWindow(Time.minutes(10))
	  .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(30)))
	  .aggregate(new Aggregate(), (
		key: String,
		window: TimeWindow,
		input: Iterable[Accumulator],
		out: Collector[Accumulator] ) => {
		  var in = input.iterator.next()
		  out.collect(Accumulator(window.getStart, window.getEnd, key, in.sum/in.count, in.count))
		})
	  .map { v =>
		val zdtFrom = new Date(v.from).toInstant().atZone(ZoneId.systemDefault())
		val from = fmt.format(zdtFrom)
		val zdtTo = new Date(v.to).toInstant().atZone(ZoneId.systemDefault())
		val to = fmt.format(zdtTo)
        val json = Map("from" -> from, "to" -> to, "username" -> v.username, "total" -> v.count, "sum" -> v.sum)
		val retval = JSONObject(json).toString()
		retval
	  }
	  .print()

    env.execute("Scala KafkaStreamCount Example")
  }
}

