package app.sparkLogs

import java.io.{BufferedWriter, File, FileWriter}

/**
 * @author yan R
 * @since 14.07.2023
 */
object LogTransform {

  def main(args: Array[String]): Unit = {
    transformLogFile("spark_f", "spark_cleansed")
  }


  def transformLogFile(inputFile: String, outputFileName: String): Unit = {

    import scala.io.Source

    val filename = s"/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/$inputFile.txt"
    val lines = Source.fromFile(filename)
      .getLines
    val list: List[String] = lines
      .map("""(\{.*?d6a"}])""".r.findFirstIn(_) match {
        case Some(value) => value.replace("abb4d6a\"}]", "abb4d6a\"},")
      })
      .toList

    val strings1 = list.+:("[")
    val str = strings1.last.dropRight(1)
    val strings2 = strings1.dropRight(1) :+ str
    val result = strings2.:+("]")

    writeFile(outputFileName, result)
  }


  def writeFile(filename: String, lines: List[String]): Unit = {
    val file = new File(s"/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/${filename}.json")
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }


}
