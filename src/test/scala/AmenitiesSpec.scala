
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class AmenitiesDataFtame(id: Long, amenities: Array[String], owner: String)
case class ResultOfAmenities(id: Long, Owner: String, NumberOfAmenities: Long)

case class GuestsAmenities(id: Long, Owner : String, Guest: String, AmenitiesLeft : Array[String])
case class RestAmenities (id : Long , Owner : String, Guest : String , NumberOfAmenitiesLeft: Long)

class AmenitiesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("theftDetection")
    .getOrCreate()

  import spark.implicits._
val amenities = new Amenities(spark)
  "getAmenitiesPerOwner" should "get the number of amenities per owner" in {
    Given("the input DataFrame")
    val input = Seq(
      AmenitiesDataFtame(6, Array("chair", "book", "table", "tv"), "Adnene"),
      AmenitiesDataFtame(2, Array("tv", "closet", "bed"), "Sayf")
    ).toDF()
    val ColumnName = "amenities"
    val Alias = "NumberOfAmenities"
    When("getAmenitiesPerOwner is invoked")
    val result = amenities.getAmenitiesPerOwner(input,ColumnName, Alias)
    Then("the number of amenities per owner should be extracted")
    val expectedResult = Seq(
      ResultOfAmenities(6, "Adnene", 4),
      ResultOfAmenities(2, "Sayf", 3)
    ).toDF()
    expectedResult.collect() should contain theSameElementsAs (result.collect())
  }
  "getAmenitiesAfterGuests" should "extract number of amenities after guests" in {
    Given("guest input DataFrame")
    val input = Seq(
      GuestsAmenities(3,"Adnene","Mohsen", Array("chair", "book","dildo")),
      GuestsAmenities(8, "Sayf","Mejdi", Array("knife"))
    ).toDF()
    When("getAmenitiesAfterGuests is invoked")
    val result = amenities.getAmenitiesAfterGuests(input).as[RestAmenities]
    Then("number of amenities after guests is extracted")
    val expectedResult = Seq(
      RestAmenities(3,"Adnene","Mohsen",3),
      RestAmenities(8, "Sayf","Mejdi",1)
    ).toDS()
    expectedResult.collect() should contain theSameElementsAs(result.collect())
  }
  "getNumberOfAmenitiesStolen" should " hfjkdhfj" in {
    Given ("inputs")
    val input =Seq(
      AmenitiesDataFtame(6, Array("chair", "book", "table", "tv"), "Adnene"),
      AmenitiesDataFtame(2, Array("tv", "closet", "bed"), "Sayf")
    ).toDF()
    val guestInput = Seq(
      GuestsAmenities(3,"Adnene","Mohsen", Array("chair", "book","dildo")),
      GuestsAmenities(8, "Sayf","Mejdi", Array("knife"))
    ).toDF()
    When("fhjk")
    val result = amenities.getNumberOfAmenitiesStolen(input, guestInput)
    Then("fhjkdf")
    result.show()
  }

}
