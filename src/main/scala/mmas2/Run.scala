package mmas2

import java.util.Date

import mmas2.Para._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


trait Run {
  val modelAnt: Ant
  var bestAnt: T_Ant

  def maxmincheck(g_Pher: Array[Array[Double]]): Unit = {
    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        if (g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if (g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }
}

/**
  * two run style:
  * no distributed & distributed
  *
  */
class Basic_run(val ANT_NUM: Int, val bestAnts: ArrayBuffer[T_Ant])
  extends Serializable with Run {

  val local_antGroup = ArrayBuffer[T_Ant]() //every iter' ants
  override val modelAnt: Ant = Util.getBestAnt(bestAnts).asInstanceOf[Ant]
  override var bestAnt: T_Ant = modelAnt

  def geneAllAntsOneIter() = {
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    for (i <- 1 to ANT_NUM) {
      val myant = new Ant(modelAnt.pher, modelAnt.stagenum,
        modelAnt.Jmax, modelAnt.dsaks, modelAnt.avss,
        modelAnt.sangs)
      myant.dealflow()
      local_antGroup.append(myant)
    }
    val best_local_ant = local_antGroup.sortWith(_.Fobj > _.Fobj).take(1)(0)
    Util.addInBestants(bestAnts, best_local_ant, best_result_num)
  }

  //update pher using the best ant
  def global_updatePher(): Unit = {
    bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  def local_updatePher() = {
    //local best ant
    bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    val best_local_ant = Util.getBestAnt(local_antGroup)

    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (best_local_ant.Fobj / faverage)

    for (i <- 0 until best_local_ant.pathlength) {
      //update
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      //reset
      if(g_Pher(i)(best_local_ant.Xdsa(i)) < pher_reset)
        g_Pher(i)(best_local_ant.Xdsa(i)) = pher_reset
    }
    maxmincheck(g_Pher)
  }
}

class Distri_run(val ANT_NUM: Int, val modelAntT: T_Ant, val globalBestAnts: AntAccumulator
                ) extends Serializable with Run {
  //init model
  override val modelAnt: Ant = modelAntT.asInstanceOf[Ant]
  override var bestAnt: T_Ant = modelAnt
  var bestLocalAnt: T_Ant = modelAnt

  def geneAllAntsOneIter(sc: SparkContext): RDD[(Int, T_Ant)] = {

    val task_num = sc.getConf.getInt("spark.default.parallelism", 1)
    val antsLocalRDD: RDD[(Int, T_Ant)] = sc.parallelize(1 to ANT_NUM, task_num)
      .map { i =>
        val myant: T_Ant = new Ant(modelAnt.pher, modelAnt.stagenum,
          modelAnt.Jmax, modelAnt.dsaks, modelAnt.avss,
          modelAnt.sangs)
        (i % task_num, myant)
      }.mapValues(ant => {
      ant.dealflow()
      ant
    })
    //find max Fobj ant
    val bestLocalAntRDD = antsLocalRDD.foldByKey(modelAnt)((a, b) => {
      if (a.Fobj >= b.Fobj)
        a
      else
        b
    })
    //action
    val best_local_ant = bestLocalAntRDD.collect()(0)._2
    globalBestAnts.add(best_local_ant)
    bestLocalAnt = best_local_ant
    antsLocalRDD
  }

  //update pher using the best ant with RDD
  def global_updatePher(antsLocalRDD: RDD[(Int, T_Ant)]): Unit = {
    //action!
    val arrayFobj: Array[(Double, Int)] = antsLocalRDD.combineByKey(
      (v: T_Ant) => (v.Fobj, 1),
      (c: (Double, Int), v: T_Ant) => (c._1 + v.Fobj, c._2 + 1),
      (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map(x => x._2).collect()
    val faverage = arrayFobj.map(_._1).sum / arrayFobj.map(_._2).sum

    bestAnt = Util.getBestAnt(this.globalBestAnts.value)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      if (g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset
    }
    //check max-min scope
    maxmincheck(g_Pher)
    this.globalBestAnts.add(bestAnt)
  }

  def local_updatePher(antsLocalRDD: RDD[(Int, T_Ant)]) = {
    //action!
    val arrayFobj: Array[(Double, Int)] = antsLocalRDD.combineByKey(
      (v: T_Ant) => (v.Fobj, 1),
      (c: (Double, Int), v: T_Ant) => (c._1 + v.Fobj, c._2 + 1),
      (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map(x => x._2).collect()
    val faverage = arrayFobj.map(_._1).sum / arrayFobj.map(_._2).sum

    bestAnt = Util.getBestAnt(this.globalBestAnts.value)
    val g_Pher = bestAnt.pher
    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (bestLocalAnt.Fobj / faverage)

    for (i <- 0 until bestLocalAnt.pathlength) {
      //update
      g_Pher(i)(bestLocalAnt.Xdsa(i)) = g_Pher(i)(bestLocalAnt.Xdsa(i)) + added
      //reset
      if (g_Pher(i)(bestLocalAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestLocalAnt.Xdsa(i)) = pher_reset
    }
    maxmincheck(g_Pher)
    //accumulator
    this.globalBestAnts.add(bestAnt)
  }
}

object Basic_run {
  //no spark operator
  def apply(iter: Int, ant_num: Int, bestants: ArrayBuffer[T_Ant],
            record: Record) {
    for (i <- 1 to iter) {
      val run = new Basic_run(ant_num, bestants)
      val starttime = new Date().getTime
      run.geneAllAntsOneIter()
      //update pher
      if (i % l_g_ratio == 0) {
        run.global_updatePher()
      } else {
        run.local_updatePher()
      }
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if(time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}

object Distri_run {

  def apply(iter: Int, ant_num: Int, modelAnt: T_Ant, globalBestAnts: AntAccumulator,
            sc: SparkContext, record: Record) {
    for (i <- 1 to iter) {
      val run = new Distri_run(ant_num, modelAnt, globalBestAnts)
      val starttime = new Date().getTime
      val localAntRDD = run.geneAllAntsOneIter(sc)
      //update pher
      if (i % l_g_ratio == 0) {
        run.global_updatePher(localAntRDD)
      } else {
        run.local_updatePher(localAntRDD)
      }
      val stoptime = new Date().getTime
      val time_interval = stoptime - starttime
      //every iter max time_interval
      if(time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}