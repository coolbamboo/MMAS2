package mmas2

import mmas2.Para.{l_g_ratio, pher0, rou}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Date
import scala.util.Random

class Distri_run(val iter: Int, val ANT_NUM: Int, val modelAntT: T_Ant, val globalBestAnts: AntAccumulator
                ) extends Serializable with Run {
  //init model
  override val modelAnt: Ant = modelAntT.asInstanceOf[Ant]
  override var bestAnt: T_Ant = modelAnt
  var bestLocalAnt: T_Ant = modelAnt

  //def geneAllAntsOneIter(sc: SparkContext): RDD[(Int, T_Ant)] = {
  def geneAllAntsOneIter(sc: SparkContext): RDD[T_Ant] = {
    val task_num = sc.getConf.getInt("spark.default.parallelism", 1)
    //val antsLocalRDD: RDD[(Int, T_Ant)] = sc.parallelize(1 to ANT_NUM, task_num)
    //val antsLocalRDD: RDD[(Double, T_Ant)] = uniformRDD(sc,ANT_NUM,task_num)
    val antsLocalRDD: RDD[T_Ant] = sc.parallelize(1 to ANT_NUM, task_num)
      .map { i =>
        //i和迭代次数作为随机数生成器的种子
        val myant: T_Ant = new Ant(modelAnt.pher, modelAnt.stagenum,
          modelAnt.Jmax, modelAnt.dsaks, modelAnt.avss,
          modelAnt.sangs, "distri")
        //(i % task_num , myant)
        val seed = iter.toString + (i + ANT_NUM).toString
        val rand = new Random(seed.toLong + new Date().getTime)
        myant.setRandom(rand)
        myant
      }.mapPartitions(ants => {
      ants.map(ant => {
        //ant.setRandom(rand)
        ant.dealflow()
        globalBestAnts.add(ant)
        ant
      }
      )
    }
    )
    //如果在这里不用下面action这种方法求最好的蚂蚁，可以在上面的代码把蚂蚁放到累加器里
    //find max Fobj ant
    /*val bestLocalAntRDD = antsLocalRDD.foldByKey(modelAnt)((a, b) => {
      if (a.Fobj >= b.Fobj)
        a
      else
        b
    })*/
    //action
    /* val best_local_ant = bestLocalAntRDD.collect().map(_._2).max(new Ordering[T_Ant] {
       def compare(a: T_Ant, b: T_Ant): Int = a.Fobj compare b.Fobj
     })*/
    val best_local_ant = antsLocalRDD.max()(new Ordering[T_Ant] {
      def compare(a: T_Ant, b: T_Ant): Int = a.Fobj compare b.Fobj
    })
    //globalBestAnts.add(best_local_ant)
    bestLocalAnt = best_local_ant
    antsLocalRDD
  }

  //}

  //update pher using the best ant with RDD
  def global_updatePher(antsLocalRDD: RDD[T_Ant]): Unit = {
    //action!
    /*val arrayFobj: Array[(Double, Int)] = antsLocalRDD.combineByKey(
      (v: T_Ant) => (v.Fobj, 1),
      (c: (Double, Int), v: T_Ant) => (c._1 + v.Fobj, c._2 + 1),
      (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map(x => x._2).collect()
    val faverage = arrayFobj.map(_._1).sum / arrayFobj.map(_._2).sum*/
    val faverage = antsLocalRDD.map(_.Fobj).reduce(_ + _) / antsLocalRDD.count()

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
    }
    //check max-min scope
    maxmincheck(g_Pher)
    this.globalBestAnts.add(bestAnt)
  }

  def local_updatePher(antsLocalRDD: RDD[T_Ant]): Unit = {
    //action!
    /*val arrayFobj: Array[(Double, Int)] = antsLocalRDD.combineByKey(
      (v: T_Ant) => (v.Fobj, 1),
      (c: (Double, Int), v: T_Ant) => (c._1 + v.Fobj, c._2 + 1),
      (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map(x => x._2).collect()
    val faverage = arrayFobj.map(_._1).sum / arrayFobj.map(_._2).sum*/
    val faverage = antsLocalRDD.map(_.Fobj).reduce(_ + _) / antsLocalRDD.count()

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
    }
    maxmincheck(g_Pher)
    this.globalBestAnts.add(bestAnt)
  }
}


object Distri_run {
  def apply(iter: Int, ant_num: Int, modelAnt: T_Ant, globalBestAnts: AntAccumulator,
            sc: SparkContext, record: Record) {
    val rand:Random = new Random(new Date().getTime)
    for (i <- 1 to iter) {
      //迭代次数传递作为随机数种子
      val run = new Distri_run(i + iter, ant_num, modelAnt, globalBestAnts)
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
      if (time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}