/*
 *  Copyright 2018 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.enceladus.kafkaplugin

import java.io.{ByteArrayOutputStream, File, IOException}
import java.util.Properties

import com.typesafe.config.Config
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.slf4j.LoggerFactory
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.plugins.api.control.{ControlMetricsPlugin, ControlMetricsPluginFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.control.NonFatal

/**
  * This is a stub for a custom implementation of a EnceladusPlugin
  */
class KafkaPlugin(props:Properties,topic_name:String) extends ControlMetricsPlugin {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def onCheckpoint(run: ControlMeasure, params: Map[String, String]):Unit = {
    val schemaJson = IOUtils.toString(this.getClass.getResourceAsStream("/info_file_avro_schema.avsc"), "UTF-8")

    val schema = new Schema.Parser().parse(schemaJson)
    val genericRecord: GenericRecord = new GenericData.Record(schema)

    // Send ControlMeasure object to Kafka
    try {

      logger.info("schema loaded")
      val arrayCheckpointSchema = schema.getField("checkpoints").schema
      logger.info("arrayCheckpointSchema" + arrayCheckpointSchema)
      //val arrayCheckpoint = new GenericData.Array[_](genericRecord.getSchema.getField("checkpoints").schema)
      val arrayCheckpoint = new GenericData.Array[GenericRecord](5, arrayCheckpointSchema)
      val itemSchemaCheckpoint = arrayCheckpointSchema.getElementType
      val nestedCheckpoint: GenericRecord = new GenericData.Record(itemSchemaCheckpoint)
      val arrayControlsSchema = itemSchemaCheckpoint.getField("controls").schema
      logger.info("arrayCheckpoint" + arrayCheckpoint)

      val cptSize = run.checkpoints.size
      val itemSchemaControls = arrayControlsSchema.getElementType
      var i = 0

      while (i < cptSize) {
        val ctrlSize = run.checkpoints(i).controls.size
        val arrayControls = new GenericData.Array[GenericRecord](ctrlSize, arrayControlsSchema)
        var j = 0
        while (j < ctrlSize) {
          val nestedControls: GenericRecord = new GenericData.Record(itemSchemaControls)
          nestedControls.put("controlName", run.checkpoints(i).controls(j).controlName)
          nestedControls.put("controlType", run.checkpoints(i).controls(j).controlType)
          nestedControls.put("controlCol", run.checkpoints(i).controls(j).controlCol)
          nestedControls.put("controlValue", run.checkpoints(i).controls(j).controlValue)
          arrayControls.add(nestedControls)
          j += 1
        }
        logger.info("arrayControls" + arrayControls)
        nestedCheckpoint.put("name", run.checkpoints(i).name)
        nestedCheckpoint.put("processStartTime", run.checkpoints(i).processStartTime)
        nestedCheckpoint.put("processEndTime", run.checkpoints(i).processEndTime)
        nestedCheckpoint.put("workflowName", run.checkpoints(i).workflowName)
        nestedCheckpoint.put("order", run.checkpoints(i).order)
        nestedCheckpoint.put("controls", arrayControls)
        arrayCheckpoint.add(nestedCheckpoint)
        i += 1
      }
      logger.info("arrayCheckpoint" + arrayCheckpoint)

      ///metadata

      val metadata = new GenericData.Record(schema.getField("metadata").schema)

      val arrayAddInfoSchema = metadata.getSchema.getField("additionalInfo").schema
      logger.info("arrayAddInfoSchema" + arrayAddInfoSchema)
      val addInfoSize = run.metadata.additionalInfo.size
      val arrayAddInfo = new GenericData.Array[GenericRecord](addInfoSize, arrayAddInfoSchema)
      val itemSchemaAddInfo = arrayAddInfoSchema.getElementType
      logger.info("itemSchemaAddInfo" + itemSchemaAddInfo)

      var k=0
      val addInfoSize1=run.metadata.additionalInfo.toList.size

      while(k<addInfoSize1) {
        val nestedAddInfo: GenericRecord = new GenericData.Record(itemSchemaAddInfo)
        nestedAddInfo.put("key", run.metadata.additionalInfo.toList(k)._1)
        nestedAddInfo.put("value", run.metadata.additionalInfo.toList(k)._2)
        logger.info("nestedAddInfo" + nestedAddInfo)
        arrayAddInfo.add(nestedAddInfo)
        logger.info("arrayAddInfo" + arrayAddInfo)
        k+=1
      }
      logger.info("arrayAddInfo" + arrayAddInfo)
      metadata.put("sourceApplication", run.metadata.sourceApplication)
      metadata.put("country", run.metadata.country)
      metadata.put("historyType", run.metadata.historyType)
      metadata.put("dataFilename", run.metadata.dataFilename)
      metadata.put("sourceType", run.metadata.sourceType)
      metadata.put("version", run.metadata.version)
      metadata.put("informationDate", run.metadata.informationDate)
      metadata.put("additionalInfo",arrayAddInfo)

      genericRecord.put("datasetName", run.metadata.sourceApplication)
      genericRecord.put("datasetVersion", run.metadata.version)
      genericRecord.put("reportDate", params("reportDate"))
      genericRecord.put("reportVersion", params("reportVersion").toInt)
      genericRecord.put("runStatus", params("runStatus"))
      genericRecord.put("metadata",metadata)
      genericRecord.put("checkpoints",arrayCheckpoint)

      logger.info("props"+props)

      logger.info("genericRecord" + genericRecord)
      val producer = new KafkaProducer[GenericRecord, GenericRecord](props)

      try {

        val keySchemaString = """{"type": "record", "name": "infoKey", "fields": [{"type": "string", "name": "key"}]}}"""
        val avroKeySchema = new Schema.Parser().parse(keySchemaString)
        val thisKeyRecord = new GenericData.Record(avroKeySchema)
        thisKeyRecord.put("key", run.metadata.sourceApplication)

        val reflectDatumWriter = new ReflectDatumWriter[Object](schema)
        val genericRecordReader = new GenericDatumReader[Object](schema)
        val t = new ByteArrayOutputStream()
        logger.info("genericRecord" + genericRecord)

        reflectDatumWriter.write(genericRecord.asInstanceOf[Object], EncoderFactory.get.directBinaryEncoder(t, null))
        val avroRecord2 = genericRecordReader.read(null, DecoderFactory.get.binaryDecoder(t.toByteArray, null)).asInstanceOf[GenericRecord]
        val record = new ProducerRecord[GenericRecord, GenericRecord](topic_name, thisKeyRecord, avroRecord2)
        producer.send(record)
      }
      catch {
        case e: IOException => {
          logger.info(e.toString)
        }
      }
      producer.close()

    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException("Unable to serialize a run object.", ex)
    }

  }
}

object KafkaPlugin extends ControlMetricsPluginFactory {
  private val logger = LoggerFactory.getLogger(this.getClass)
  var props = new Properties()
  def validateConfig(conf:Config): Unit ={

  }
  override def apply(conf: Config): ControlMetricsPlugin = {
    // Here an instance of Kafka Producer an be built based on the configuration passed as `conf`
    // Configuration can be something like this. Probably a validation is needed to ensure all required parameters have been passed.
      logger.info("conf"+conf)
      val BOOTSTRAP_SERVERS_CONFIG = conf.getString("kafka.bootstrap.servers")
      val SCHEMA_REGISTRY_URL_CONFIG = conf.getString("kafka.schema.registry.url")
      val CLIENT_ID_CONFIG = conf.getString("kafka.client.id")
      val KEY_SERIALIZER_CLASS_CONFIG = conf.getString("kafka.key.serializer")
      val VALUE_SERIALIZER_CLASS_CONFIG = conf.getString("kafka.value.serializer")
      val TOPIC_NAME = conf.getString("kafka.topic.name")

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG)
      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_CONFIG)
      props.put(CommonClientConfigs.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG)

      logger.info("Building Kafka plugin")

      return new KafkaPlugin(props,TOPIC_NAME)
    }
}
