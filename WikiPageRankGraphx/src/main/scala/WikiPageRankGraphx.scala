
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
/**
 * @author rakeshdrk
 */
object WikiPageRankGraphx {
  
  sealed trait Seal
  case class VertexProp(id:Int, name:String, body:String) extends Seal
  
   def main(args: Array[String]): Unit = {
     
     var path = "s3://rdammala/Assig-2/input/freebase-wex-2009-01-12-articles.tsv";
     var univs= "false";
     var output = "output-1";
     var univspath = "s3://rdammala/Assig-2/input/univslist";
     if(args.length == 4)
     {
        path = args(0)
        output = args(1)
        univs = args(2)
        univspath = args(3)
     } else if(args.length == 2)
     {
        path = args(0)
        output = args(1)
     } else if(args.length == 1)
     {
        path = args(0)
     }
     var t1 = System.currentTimeMillis()
     val sconf = new SparkConf()
     //sconf.setMaster("local").setAppName("WikipediaPageRank")
     val sc: SparkContext = new SparkContext(sconf)
     val input: RDD[String] = sc.textFile(path)
     val vertices = input.map(x => x.split('\t')).filter(x => (x.length > 1 && !(x(1) contains "REDIRECT"))).
         map(x => ((pageId(x(1)), {val id = x(0).toInt
                      val name = x(1)
                      val body = x(3)
                      new VertexProp(id,name,body)
         }))).cache
     
     val pattern = "<target>.+?</target>".r
     val edges = vertices.flatMap(
                    x =>
                    pattern.findAllIn(x._2.body).map { y =>
                    val dst = pageId(y.replace("<target>","").replace("</target>",""))
                    Edge(x._1, dst, 1.0)
                  }
                ).cache
     
     val graph = Graph(vertices,edges)
     val pagerank = graph.staticPageRank(5).vertices
     
     val ranksByName = vertices.join(pagerank).map {
        case (id, (vertex, rank)) => (vertex.name, rank)
     }
     if(univs.equalsIgnoreCase("false"))
     {
       var t2 = System.currentTimeMillis()
       var totalTime = t2-t1
       var output1 = output+"output-1"
       //Files.deleteIfExists(Paths.get(output1))
       var ranksSorted = ranksByName.sortBy(x => x._2,false)
       ranksSorted.saveAsTextFile(output1)
       ranksSorted.take(100).foreach(a => {
         val str = a._1+":"+a._2+" \n"
         println(str)
         //Files.write(Paths.get(output1+"/output1.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
         })
       val str = "Total time taken in GraphX:" + totalTime  
       println(str)
       println("********************************************************************")
       calcTop100univs(ranksByName,sc,output,System.currentTimeMillis(),univspath);
       //Files.write(Paths.get(output1+"/output1.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
     }
     if(univs.equalsIgnoreCase("true"))  
     {
       
       calcTop100univs(ranksByName,sc,output,t1,univspath); 
     }
  }
  
  def calcTop100univs(name: RDD[(String, Double)],sc :SparkContext,output : String, t1: Long, univspath: String) : Unit = {
     val univs = sc.textFile(univspath).map(x => (x,0.0))
     val univslist =  name.join(univs).map(x => (x._1,x._2._1))
     var t2 = System.currentTimeMillis()
     var totalTime = t2-t1
     var output3 = output+"output-3"
     //Files.deleteIfExists(Paths.get(output3))
     var univsSorted = univslist.sortBy(x => x._2, false)
     univsSorted.saveAsTextFile(output3)
     var results = univsSorted.take(100)
     results.foreach(a => {
       val str = a._1+":"+a._2+" \n"
       //Files.write(Paths.get(output3+"/output3.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
       println(str)})
     
     val str = "Total time taken to list top 100 universitites:" + totalTime  
     println(str)
     Files.write(Paths.get(output3+"/output3.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
  
  
  def pageId(name: String): VertexId = {
    name.toLowerCase.trim.hashCode.toLong
  }
  
}