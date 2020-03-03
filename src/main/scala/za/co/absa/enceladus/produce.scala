package za.co.absa.enceladus

import com.typesafe.config.ConfigFactory
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}
import za.co.absa.enceladus.kafkaplugin.KafkaPlugin

object produce{

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("application.conf")
    val params: Map[String,String]=Map("datasetName" -> "PAYM_NAEDO"
      ,"datasetVersion"->"1"
      ,"reportDate" -> "2020-02-21"
      , "reportVersion" -> "1"
      ,"runStatus"->"Completed"
      ,"kafkaProp"->"kafka")

    val additionalInfo=Map("key1"->"test","value1"->"test")
    val measureMetadata = ControlMeasureMetadata("PAYM","ZA","SNAPSHOT","TEST","MAINFRAME",1,"2020-02-21",additionalInfo)
    val measureMetadata1 = ControlMeasureMetadata("PAYM","ZA","SNAPSHOT","TEST","MAINFRAME",2,"2020-02-21",additionalInfo)
    val control= List(Measurement("count","test","test","test"))
    val checkpoint = List(Checkpoint("raw","test","test","test",1,control))
    val runUniqueId:Option[String] = Some("test")
    val measure = ControlMeasure(measureMetadata,runUniqueId,checkpoint)
    val measure1 = ControlMeasure(measureMetadata1,runUniqueId,checkpoint)
    val plugin = KafkaPlugin.apply(conf)
    plugin.onCheckpoint(measure,params)
    plugin.onCheckpoint(measure1,params)

  }

}
