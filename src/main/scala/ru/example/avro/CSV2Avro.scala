package ru.example.avro

import com.typesafe.config.ConfigFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.apache.commons.csv._
import java.io.{File, FileReader, PrintWriter}
import scala.jdk.CollectionConverters._

object CSV2Avro {

  case class HeaderType(name: String, `type`: String)

  def main(args: Array[String]): Unit = {
    // Читаем конфигурационный файл
    val config = ConfigFactory.load()

    val inputFileName = config.getString("input")
    val name = config.getString("name")
    val namespace = config.getString("namespace")
    val outputFileName = config.getString("output")

    // Читаем файл с данными
    val in = new FileReader(inputFileName)

    // Получаем записи
    val csvFormat = CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).build()
    val records = csvFormat.parse(in)

    // Извлекаем заголовок и первую строку
    val headerNames = records.getHeaderNames.asScala
    val firstLine: CSVRecord = records.iterator().next()

    val headerTypes = headerNames.map {h =>
      val name = h.replaceAll(" ", "")
      val value = firstLine.get(h)
      val valueType = getType(value)
      HeaderType(name, valueType)
    }.toList

    // Формируем описание схемы в JSON
    val schemaJSON = defineSchema(headerTypes, name, namespace)

    // Формируем схему для Avro
    val schema = new Schema.Parser().parse(schemaJSON)

    // Сохраняем схему в AVSC
    new PrintWriter(s"$name.avsc") { write(schema.toString); close() }

    // Выводим записи в формате Avro
    val file = new File(outputFileName)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    dataFileWriter.create(schema, file)
    dataFileWriter.append(createRecord(firstLine, headerTypes, schema))
    records.forEach { record => dataFileWriter.append(createRecord(record, headerTypes, schema)) }
    dataFileWriter.flush()
    dataFileWriter.close()
  }

  def isLong(value: String): Boolean = {
    val long = "^-?\\d{1,19}$".r
    long.findFirstIn(value).isDefined
  }

  def isDouble(value: String): Boolean = {
    val double = "^(-?)(0|([1-9][0-9]*))(\\.[0-9]+)?$".r
    double.findFirstIn(value).isDefined
  }

  def getType(value: String): String = {
    if (isLong(value)) {
      val number = value.toLong
      if (number > -2147483648 & number < 2147483647) "int"
      else "long"
    } else if (isDouble(value)) "double"
    else "string"
  }

  def defineSchema(headerTypes: List[HeaderType], name: String, nameSpace: String): String = {
    val fields = headerTypes.map { ht =>
      s"    {\"name\": \"${ht.name}\", \"type\": [\"${ht.`type`}\", \"null\"]}"
    }.mkString(",\n")

    val schema = s"""{
  \"namespace\": \"$nameSpace\",
  \"type\": \"record\",
  \"name\": \"$name\",
  \"fields\": [
$fields
  ]
}"""
    schema
  }

  def createRecord(csvRecord: CSVRecord, headerTypes: List[HeaderType], schema: Schema): GenericRecord = {
    val record = new GenericData.Record(schema)
    headerTypes.foreach { ht =>
      val csvValue: String = csvRecord.get(ht.name)
      val value = ht.`type` match {
        case "int" => if (isLong(csvValue)) csvValue.toInt else null
        case "long" => if (isLong(csvValue)) csvValue.toLong else null
        case "double" => if (isDouble(csvValue)) csvValue.toDouble else null
        case _ => csvValue
      }
      record.put(ht.name, value)
    }
    record
  }
}
