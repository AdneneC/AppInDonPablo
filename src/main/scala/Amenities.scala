import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Amenities (spark : SparkSession) {
  import spark.implicits._
def getAmenitiesPerOwner(input: DataFrame, ColumnName : String , Alias : String ) : DataFrame = {

  input.select($"id",
    $"Owner",
    size(col(ColumnName)).as(Alias)
  )

}
  def getAmenitiesAfterGuests (guestAmenities : DataFrame): DataFrame = {

    val amenitiesDataFrame = getAmenitiesPerOwner(guestAmenities, "AmenitiesLeft", "NumberOfAmenitiesLeft")
      val guestDataFrame = guestAmenities.select("id", "Guest")
    amenitiesDataFrame.join(guestDataFrame, "id")
  }
  def getNumberOfAmenitiesStolen(input : DataFrame, guestAmenities: DataFrame) : DataFrame = {
    val beforeRent = getAmenitiesPerOwner(input, "Amenities", "NumberOfAmenities")
    val afterRent = getAmenitiesAfterGuests(guestAmenities)
    beforeRent.join(afterRent, "Owner").select(
      "Owner",
      "Guest",
      "NumberOfAmenities",
      "NumberOfAmenitiesLeft"
    ).where(col("NumberOfAmenities") > col("NumberOfAmenitiesLeft"))
      .withColumn("NumberOfAmenitiesStolen", col("NumberOfAmenities") - col("NumberOfAmenitiesLeft"))
      .drop("NumberOfAmenities", "NumberOfAmenitiesLeft")
  }
}
