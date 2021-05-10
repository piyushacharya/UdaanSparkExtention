package capillary

import scala.collection.mutable
import scala.io.Source

class OtherUtils {

}

class ReviewUtilities
{
  def createNodeAndEdge(): Unit ={
    var header = true
    val linkfilename = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /orch/DAG Metadata /neo4j_dump.csv"


    import java.io._
    val nodeLink_csv = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /Analysis/edges.csv"
    val noderepo_csv = "/Users/piyush.acharya/MyWorkSpace/Databricks/Projects /03 Capillary /Analysis/nodes.csv"

    val nlpw = new PrintWriter(new File(nodeLink_csv))
    val nrpw = new PrintWriter(new File(noderepo_csv))
    val repo: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()
    var keyIndex: Long = 0;

    nlpw.write(s"Source,Target,Type,Weight\n")

    for (line <- Source.fromFile(linkfilename).getLines()) {
      if (line.contains("100443")) {
        if (header) {
          header = false
          print(line)
        } else {

          var parentNodeIndex: Long = 0
          var childNodeIndex: Long = 0
          var parentNodeName: String = "null"
          var childNodeName: String = "null"

          val splits = line.split(",")
          val parentKey: String = splits(0)

          if (parentKey != "null") {
            val parentKeyParts = parentKey.split("/")
            val sreachKey = parentKeyParts(1)

            if (!repo.contains(sreachKey)) {
              keyIndex = keyIndex + 1
              repo.put(sreachKey, keyIndex)
              parentNodeIndex = keyIndex
            } else {
              parentNodeIndex = repo.get(sreachKey).get
            }
            parentNodeName = sreachKey
          }

          val childKey = splits(2)

          if (childKey != "null") {
            val childKeyParts = childKey.split("/")
            // Child key
            val sreachKey = childKeyParts(1)
            if (!repo.contains(sreachKey)) {
              keyIndex = keyIndex + 1
              repo.put(sreachKey, keyIndex)
              childNodeIndex = keyIndex
            } else {
              childNodeIndex = repo.get(sreachKey).get
            }
            childNodeName = sreachKey
          }
          if (parentNodeName == "null" && childNodeName == "null")
            println(s"$line")

          if (parentNodeIndex > 0 && childNodeIndex > 0)
            nlpw.write(s"$parentNodeIndex,$childNodeIndex,Directed,0.1\n")
        }
      }
    }
    nlpw.close

    nrpw.write("Id,Label\n")
    for (elem <- repo)
      nrpw.write(s"${elem._2},${elem._1}\n")

    nrpw.close()

  }

}