package mmas2

import mmas2.Para._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import java.io.{FileOutputStream, InputStream, OutputStream}
import java.net.URI
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer

/**
  * tool methods
  */
object Util {
  def morethan(first: Array[Int], second: Array[Int]): Boolean = {
    if (first.length != second.length) throw new Exception("compared array length not equal")
    var temp = false
    for (i <- first.indices) {
      //one more than , true
      if (first(i) > second(i)) {
        temp = true
        return temp
      }
    }
    temp
  }

  def getBestAnt(bestants: ArrayBuffer[T_Ant]): T_Ant = {
    bestants.max(new Ordering[T_Ant] {
      def compare(a: T_Ant, b: T_Ant): Int = a.Fobj compare b.Fobj
    })
  }

  def addInBestants(bestAnts: ArrayBuffer[T_Ant], myant: T_Ant, length_max: Int): Unit = {
    if (bestAnts.length < length_max) {
      bestAnts.append(myant)
    }
    else {
      //if better than bestAnts,update
      var min_index = 0
      var minobj = Double.MaxValue
      for (i <- bestAnts.indices) {
        if (bestAnts(i).Fobj < minobj) {
          min_index = i
          minobj = bestAnts(i).Fobj
        }
      }
      if (myant.Fobj >= minobj) {
        bestAnts.synchronized {
          bestAnts.update(min_index, myant)
        }
      }
    }
  }

  def saveToLocal(outputs : Vector[Output], outputfile : String = "./results.csv"): Unit ={
    import java.io.{BufferedWriter, FileOutputStream, IOException, OutputStreamWriter}
    var out : BufferedWriter = null
    try {
      out = new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream(outputfile, true)))
      outputs.foreach(output => {
        out.write(output.toString() + "\r\n")
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally try
      out.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  //compute J's max bound
  def deal_Jup(stagenum: Int, avss: Array[AVS], dsaks: Array[DSAK_Jup]): (Array[DSAK_Jup], Array[AVS]) = {

    val Vdsak_j = dsaks.map {
      case DSAK_Jup(num, d, s, a, kdsa, _) =>
        val gs = avss.filter(x => x.s == s).map(x => x.Gs)
        val jup = Array(
          B(stagenum)(d - 1), gs(0) / t(stagenum)(d - 1), Cmax(stagenum) / c(stagenum)(d - 1), M(stagenum)(d - 1) / m(stagenum)(d - 1)
        ).min
        if (jup < 0) throw new Exception("jup is wrong！")
        DSAK_Jup(num, d, s, a, kdsa, jup)
    }
    (Vdsak_j, avss)
  }

  def randval(a: Double, b:Double) :Double = {
    val r = ThreadLocalRandom.current().nextDouble(1.0)
    val value = a + (b - a) * r
    value
  }
}