import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
 * @author rakeshdrk
 */
object WikiPageRankSpark {
  
     sealed trait Seal
     case class VertexProp(id:Int, name:String, body:String) extends Seal
     
     def main(args: Array[String]): Unit = {
     
         var path = "s3://rdammala/Assig-2/input/freebase-wex-2009-01-12-articles.tsv"; 
         var output = "output-2"
         if(args.length >= 2)
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
    
         val mapper = input.map(x => x.split('\t')).filter(x => (x.length > 1 && !(x(1) contains "REDIRECT"))).map(x => (x(1), x(3))).cache //pageId(x(1))
             
         var vertices = mapper.map(x => (x._1,1.0)) //default rank 1
         println(vertices.count);
         val pattern = "<target>.+?</target>".r
         val edges = mapper.flatMap(
                    x =>
                    pattern.findAllIn(x._2).map { y =>
                      (x._1,y.replace("<target>","").replace("</target>","")) 
                  }
                ).distinct().groupByKey().cache()

         for (i <- 1 to 5) {
             var contribs  = edges.join(vertices).values.flatMap {
               case (edge,rank) => 
                 val size = edge.size
                 edge.map(e => {(e,rank/size)}) 
             }
          vertices = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
        }
        var t2 = System.currentTimeMillis()
        var timeTaken = t2-t1
        var sortedvertices = vertices.sortBy(x => x._2,false)
        sortedvertices.saveAsTextFile(output)
        sortedvertices.take(100).foreach(a => {
         val str = a._1+":"+a._2+" \n"
         println(str)
         //Files.write(Paths.get(output+"/output2.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
         })
         val str = "Total time taken in Spark:" + timeTaken  
         println(str)
         //Files.write(Paths.get(output+"/output2.txt"), str.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
      
  def pageId(name: String): Long = {
        name.toLowerCase.trim.hashCode.toLong
  }
     
}