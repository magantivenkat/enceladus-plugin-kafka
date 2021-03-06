/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.plugins.api.control

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.plugins.api.Plugin

/**
 * Base class for all Enceladus external plugins that process control measurements.
 */
abstract class ControlMetricsPlugin extends Plugin {

  /**
   * This callback function will be invoked each time a checkpoint is created or a job status changes.
   *
   * @param measurements An object containing all control measurements (aka INFO file).
   * @param params       Additional key/value parameters provided by Enceladus.
   */
  def onCheckpoint(measurements: ControlMeasure, params: Map[String, String]): Unit

}
