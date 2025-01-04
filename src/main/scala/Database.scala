import org.mongodb.scala._
import scala.concurrent.Await
import scala.concurrent.duration._

object Database {
  val uri: String="mongodb+srv://teamtp10:test123@cluster0.5jb5h.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
  val client: MongoClient=MongoClient(uri)
  val database: MongoDatabase=client.getDatabase("twitter_db")
  val collection: MongoCollection[Document]=database.getCollection("tweets")

  def insertDocument(data: Map[String,Any]):Unit={
    val document=Document(data.mapValues(_.toString))
    try{
      val insertObservable= collection.insertOne(document)
      val insertResult= Await.result(insertObservable.toFuture(),10.seconds)
      println(s"Successfully inserted document: $insertResult")
    }catch {
      case e:Throwable =>println(s"Failed to insert document: ${e.getMessage}")
    }
  }

  def close():Unit= client.close()
}
