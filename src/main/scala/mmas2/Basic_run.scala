package mmas2

import mmas2.Para.{best_result_num, l_g_ratio, pher0, pher_reset, rou}

import java.util.Date
import scala.collection.mutable.ArrayBuffer


class Basic_run(val ANT_NUM: Int, val bestAnts: ArrayBuffer[T_Ant])
  extends Serializable with Run {

  //初始化，每次迭代开始时的初始化
  val local_antGroup: ArrayBuffer[T_Ant] = ArrayBuffer[T_Ant]() //every iter' ants
  //用当前最好的蚂蚁（本次迭代前）做模板
  override val modelAnt: Ant = Util.getBestAnt(bestAnts).asInstanceOf[Ant]
  //当前最好的蚂蚁（本次迭代前）
  override var bestAnt: T_Ant = modelAnt

  //本地迭代，先生成所有蚂蚁
  def geneAllAntsOneIter(): Unit = {
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    for (_ <- 1 to ANT_NUM) {
      val myant = new Ant(modelAnt.pher, modelAnt.stagenum,
        modelAnt.Jmax, modelAnt.dsaks, modelAnt.avss,
        modelAnt.sangs)
      myant.dealflow()
      local_antGroup.append(myant)
    }
    //从本地迭代生成的所有蚂蚁中选出最好的
    val best_local_ant = local_antGroup.sortWith(_.Fobj > _.Fobj).take(1)(0)
    //加入全局最好的蚂蚁列表
    Util.addInBestants(bestAnts, best_local_ant, best_result_num)
  }

  //update pher using the best ant
  def global_updatePher(): Unit = {
    //本次迭代后的最好蚂蚁（全局）
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
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  def local_updatePher(): Unit = {
    //本次迭代后的最好蚂蚁（全局）
    bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    //本次迭代后生成的最好蚂蚁（本次迭代，局部）
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
    }
    maxmincheck(g_Pher)
  }
}

object Basic_run {
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
      if (time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}