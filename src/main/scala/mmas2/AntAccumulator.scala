package mmas2

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

class AntAccumulator(private val lenth_max: Int) extends AccumulatorV2[T_Ant, ArrayBuffer[T_Ant]] {

  private var bestAnts: ArrayBuffer[T_Ant] = ArrayBuffer[T_Ant]()
  private val bound = lenth_max

  override def isZero: Boolean = bestAnts.isEmpty

  override def copy(): AccumulatorV2[T_Ant, ArrayBuffer[T_Ant]] = {
    val newAcc = new AntAccumulator(bound)
    bestAnts.synchronized {
      newAcc.bestAnts.appendAll(bestAnts)
    }
    newAcc
  }

  override def reset(): Unit = {
    bestAnts.clear()
  }

  override def add(v: T_Ant): Unit = {
    Util.addInBestants(bestAnts, v, bound)
  }

  override def merge(other: AccumulatorV2[T_Ant, ArrayBuffer[T_Ant]]): Unit = {
    other match {
      case o: AntAccumulator =>
        bestAnts.appendAll(o.value)
        if (bestAnts.length > bound)
        //sort and slice
          bestAnts = bestAnts.sortWith((a, b) => a.Fobj > b.Fobj).take(bound)
    }
  }

  override def value: ArrayBuffer[T_Ant] = bestAnts
}
