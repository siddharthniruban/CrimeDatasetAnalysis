import org.apache.spark.sql.DataFrame

import java.io.{BufferedWriter, File, FileWriter}

class CommonUtil {
  def DFwriteToFileIntLong(df:DataFrame,outputPath:String):Unit={
    val filewrite = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(filewrite,true))
    df.collect.foreach{ l =>
      bw.write(l.getInt(0)+" | "+l.getLong(1)+"\n")
    }
    bw.close()
  }

  def DFwriteToFileStringLong(df:DataFrame,outputPath:String):Unit={
    val filewrite = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(filewrite,true))
    df.collect.foreach{ l =>
      bw.write(l.getString(0)+" | "+l.getLong(1)+"\n")
    }
    bw.close()
  }

  def DFwriteToFileStringStringLong(df:DataFrame,outputPath:String):Unit={
    val filewrite = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(filewrite,true))
    df.collect.foreach{ l =>
      bw.write(l.getString(0)+" | "+l.getString(1)+" | "+l.getLong(2)+"\n")
    }
    bw.close()
  }

  def writeToFileString(str:String,outputPath:String):Unit={
    val filewrite = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(filewrite,true))
    bw.write(str+"\n")
    bw.close()
  }

}
