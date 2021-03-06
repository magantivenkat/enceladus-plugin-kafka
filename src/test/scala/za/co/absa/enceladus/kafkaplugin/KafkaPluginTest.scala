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


import java.io.File
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, WordSpec}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}

class KafkaPluginTest {

    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource("info_file_avro_schema.avsc").getFile).toPath.toString
    //val config=new File(classLoader.getResource("application.conf").getFile).toPath.toString
    val conf = ConfigFactory.load("application.conf")
    val params: Map[String, String] = Map("datasetName" -> "PAYM_NAEDO"
      ,"datasetVersion"->"1"
      ,"reportDate" -> "2020-02-21"
      , "reportVersion" -> "1"
      , "runStatus" -> "Completed"
      , "schemaFile" -> file
      , "kafkaProp" -> "kafka")
    val additionalInfo = Map("key1" -> "test", "value1" -> "test")
    val measureMetadata = ControlMeasureMetadata("PAYM", "ZA", "SNAPSHOT", "TEST", "MAINFRAME", 1, "2020-02-21", additionalInfo)
    val control = List(Measurement("count", "test", "test", "test"))
    val checkpoint = List(Checkpoint("raw", "test", "test", "test", 1, control))
    val runUniqueId: Option[String] = Some("test")
    val measure = ControlMeasure(measureMetadata, runUniqueId, checkpoint)

    val message = "testMessage"

    val plugin = KafkaPlugin.apply(conf)
    plugin.onCheckpoint(measure, params)

}

