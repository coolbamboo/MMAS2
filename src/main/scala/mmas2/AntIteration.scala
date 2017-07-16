package MMAS

import java.util.Date

import mmas2._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import mmas2.common._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
/**
 * Created by root on 2016/3/6.
 *
 */
class AntIteration(val stagenum:Int, val ANT_NUM:Int, val bestAnts: ArrayBuffer[T_Ant],
                   J_max:Int, dsak_j:Array[DSAK_Jup], avs:Array[AVS],
                   sang:Array[SANG], val i_iter: Long) extends Serializable {

  val local_antGroup = scala.collection.mutable.ArrayBuffer[T_Ant]() //every iter' ants

  def geneAllAntsOneIter(sc: SparkContext, par: Int) :RDD[T_Ant] = {
    val bestant: T_Ant = Util.getBestAnt(bestAnts)
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    val ant_seq_RDD: RDD[T_Ant] = sc.parallelize(1 to ANT_NUM, par).map{i=>
      val myant:T_Ant = new Ant(bestant.pher, stagenum, J_max, dsak_j, avs, sang)
      myant.dealflow()
      myant
    }.sortBy(_.Fobj,false)
    ant_seq_RDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    //action!
    val best_local_ant = ant_seq_RDD.take(1)(0)
    Util.addInBestants(bestAnts, best_local_ant)
    ant_seq_RDD
  }

  def geneAllAntsOneIter() = {
    val bestant: T_Ant = Util.getBestAnt(bestAnts)
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    for (i <- 1 to ANT_NUM) {
      val myant = new Ant(bestant.pher, stagenum, J_max, dsak_j, avs, sang)
      myant.dealflow()
      local_antGroup.append(myant)
    }
    val best_local_ant = local_antGroup.sortWith(_.Fobj > _.Fobj).take(1)(0)
    Util.addInBestants(bestAnts, best_local_ant)
  }

  //update pher using the best ant
  def global_updatePher(): Unit = {
    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength - 1 if bestAnt.pathlength < stagenum) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
    }
    if (bestAnt.pathlength == stagenum) {
      for (i <- 0 until bestAnt.pathlength) {
        g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
        //reset
        /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
          g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
      }
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  private def maxmincheck(g_Pher:Array[Array[Double]]): Unit = {
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        if (g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if (g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }

  def local_updatePher() = {
    //local best ant
    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    val best_local_ant = Util.getBestAnt(local_antGroup)

    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (best_local_ant.Fobj / faverage)

    for (i <- 0 until best_local_ant.pathlength - 1 if best_local_ant.pathlength < stagenum) {
      //update
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
    }
    if (best_local_ant.pathlength == stagenum) {
      for (i <- 0 until best_local_ant.pathlength) {
        g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      }
    }
    maxmincheck(g_Pher)
  }

  //update pher using the best ant with RDD
  def global_updatePher(ants : RDD[T_Ant]): Unit = {
    //action!
    val antsFogj = ants.map(ant => ant.Fobj)
    val faverage = antsFogj.reduce(_+_) / antsFogj.count()

    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength - 1 if bestAnt.pathlength < stagenum) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
    }
    if (bestAnt.pathlength == stagenum) {
      for (i <- 0 until bestAnt.pathlength) {
        g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
        //reset
        /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
          g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
      }
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  //no need local_antGroup
  def local_updatePher(ants : RDD[T_Ant]) = {
    //local best ant
    val best_local_ant = ants.take(1)(0)
    val antsFobj = ants.map(ant => ant.Fobj)
    //action!
    val faverage = antsFobj.reduce(_+_) / antsFobj.count()

    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (best_local_ant.Fobj / faverage)

    for (i <- 0 until best_local_ant.pathlength - 1 if best_local_ant.pathlength < stagenum) {
      //update
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
    }
    if (best_local_ant.pathlength == stagenum) {
      for (i <- 0 until best_local_ant.pathlength) {
        g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      }
    }
    maxmincheck(g_Pher)
  }
}

object AntIteration {

  def apply(stagenum:Int, iter:Int, ant_num:Int, bestants : ArrayBuffer[T_Ant],
            J_max:Int, dsak_j:Array[DSAK_Jup], avs:Array[AVS],
            sang:Array[SANG], sc : SparkContext, par: Int, is_par:String,
            record:Record){
    for (i <- 1 to iter){
      val antoneiter = new AntIteration(stagenum, ant_num, bestants ,J_max,
        dsak_j, avs, sang, i)
      val starttime = new Date().getTime
      is_par match {
        case "" => {
          antoneiter.geneAllAntsOneIter()
          //update pher
          if (antoneiter.i_iter % l_g_ratio == 0) {
            antoneiter.global_updatePher()
          } else {
            antoneiter.local_updatePher()
          }
        }
        case _ => {
          val ants = antoneiter.geneAllAntsOneIter(sc,par)
          //update pher
          if (antoneiter.i_iter % l_g_ratio == 0) {
            antoneiter.global_updatePher(ants)
          } else {
            antoneiter.local_updatePher(ants)
          }
        }
      }
      val stoptime = new Date().getTime
      val time_interval = stoptime-starttime
      //every iter max time_interval
      if(time_interval > record.time_everyiter)
        record.time_everyiter = time_interval
    }
  }
}