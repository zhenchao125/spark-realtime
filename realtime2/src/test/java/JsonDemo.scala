import org.json4s.jackson.JsonMethods

/**
  * Author lzc
  * Date 2019-09-27 15:13
  */
object JsonDemo {
    def main(args: Array[String]): Unit = {
        val list = List(("a", 1), ("B", 2), ("c", 3))
        import org.json4s.JsonDSL._
        println(JsonMethods.compact(JsonMethods.render(list)))
        
    }
}
