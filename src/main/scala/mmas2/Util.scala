package mmas2

import java.io._

import mmas2.common._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by root on 2016/4/6.
 */
object Util {
  def morethan(first:Array[Int], second:Array[Int]):Boolean={
    if(first.length != second.length) throw new Exception("compared array length not equal")
    var temp = false
    for(i <- 0 until first.length){
      //one more than , true
      if(first(i) > second(i)) {
        temp = true
        return temp
      }
    }
    temp
  }

  def getBestAnt(bestants : ArrayBuffer[T_Ant]) : T_Ant = {
    bestants.max(new Ordering[T_Ant] {
      def compare(a: T_Ant, b: T_Ant) = a.Fobj compare b.Fobj
    })
  }

  def getBestAnt(bestants : RDD[T_Ant]) : T_Ant = {
    bestants.collect().max(new Ordering[T_Ant] {
      def compare(a: T_Ant, b: T_Ant) = a.Fobj compare b.Fobj
    })
  }

  def addInBestants(bestAnts:ArrayBuffer[T_Ant], myant:T_Ant) :Unit = {
    if (bestAnts.length < best_result_num) {
      bestAnts.append(myant)
    }
    else {
      //if better than bestAnts,update
      var min_index = 0
      var minobj = Double.MaxValue
      for (i <- 0 until bestAnts.length) {
        if (bestAnts(i).Fobj < minobj) {
          min_index = i
          minobj = bestAnts(i).Fobj
        }
      }
      if (myant.Fobj > minobj) {
        bestAnts.update(min_index, myant)
      }
    }
  }

  def saveToLocal(outputs : Vector[Output]): Unit ={
    import java.io.{BufferedWriter, FileOutputStream, IOException, OutputStreamWriter}
    var out : BufferedWriter = null
    try {
      out = new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream("./results.csv", true)))
      outputs.map(output => {
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
}

// This wrapper lets us update brodcast variables
// without running into serialization issues
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val sc: SparkContext,
                                          @transient private val _v: T) {

  @transient private var v = sc.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = sc.broadcast(newValue)
  }

  def value: T = v.value

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
}