import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}

object HiveTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "warehouse")
      .getOrCreate()

    import spark.implicits._
    val database = "dv"
    val table = "test"
    val df = Seq(("A",1),("B", 2), ("C", 3)).toDF("name", "id")

    val catalog = spark.sharedState.externalCatalog

    spark.sql(s"create database if not exists $database")

    catalog.createTable(
      CatalogTable(
        TableIdentifier(table, Option(database)),
        CatalogTableType.EXTERNAL,
        CatalogStorageFormat.empty,
        df.schema,
        provider = Option("ORC"),
        partitionColumnNames = Seq("name")
      ),
      false)

    spark.catalog.refreshTable(s"$database.$table")

    println(catalog.getTable(database, table))

//    df.write.partitionBy("name").saveAsTable(s"$database.$table")

//    spark.table(s"$database.$table").show()
  }

}
