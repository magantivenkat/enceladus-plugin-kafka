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

import org.apache.commons.configuration.Configuration
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.api.{EnceladusPlugin, EnceladusPluginFactory}
import za.co.absa.enceladus.model.Run

/**
  * This is a stub for a custom implementation of a EnceladusPlugin
  */
class KafkaPlugin extends EnceladusPlugin {

  override def onCheckpoint(run: Run, params: Map[String, String]): Unit = {
    // Send run object to Kafka

    /*
    try {
      val genericRecord: GenericRecord = new GenericData.Record(schema)
      genericRecord.put("dataset", run.dataset)
      genericRecord.put("datasetVersion", run.datasetVersion)
      genericRecord.put("reportDate", params.get("reportDate"))
      genericRecord.put("reportVersion", params.get("reportVersion").parseInt)
      genericRecord.put("runStatus", run.runStatus)
      genericRecord
    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException("Unable to serialize a run object.", ex)
    }

     */
  }

}

object KafkaPlugin extends EnceladusPluginFactory {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(conf: Configuration): EnceladusPlugin = {

    // Here an instance of Kafka Producer an be built based on the configuration passed as `conf`

    // Configuration can be something like this. Probably a validation is needed to ensure all required parameters have been passed.

    /*
    enceladus.kafka.schema.registry.url=http://zapalnrapp1079.corp.dsarena.com:8081
    enceladus.kafka.bootstrap.servers=zapalnrapp1078.corp.dsarena.com:9092
    enceladus.kafka.client.id=testInfo1
    enceladus.kafka.topic.name=global.info
      ...
     */

    logger.info("Building Kafka plugin")
    new KafkaPlugin()
  }
}
