import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    val df = Seq(1, 2, 3).toDF("id")

//    df.show()
    df.write
      .format("avro")
      .option("avroSchema", SchemaConverters.toAvroType(df.schema).toString)
      .mode(SaveMode.Overwrite)
      .save("test_avro")
  }
}