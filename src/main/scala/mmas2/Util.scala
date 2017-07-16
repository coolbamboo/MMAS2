package mmas2

import scala.collection.mutable.ArrayBuffer
import common._
import org.apache.spark.rdd.RDD

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
}
