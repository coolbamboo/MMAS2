package mmas2


import java.util.Date

import mmas2.common._
import org.apache.spark.{SparkConf, SparkContext}

//append dfs
/**
 * Created by root on 2016/3/6.
  * imported parameter:
  * args0: stage num(variable num)
  * args1: iteration num
  * args2: ant num(every iteration)
  * args3: task_3(whether partition and how many pars,0 is use no concurrency)
  * args4: localpath,data file is sourcing from local(fileurl) or "hdfs"
  * args5: "local" running , or other running style
  * args6: basic run MMAS, or improved(distributed) run MMAS
  * args7: result:"local" or save on "hdfs"
 */
object main {
  def main(args: Array[String]) {
    val stagenum:Int = args(0).toInt
    val iter_num:Int = args(1).toInt
    val ant_num:Int = args(2).toInt
    val task_in = args(3)
    val is_task = task_in.trim.split("_")(0)
    val task_num = task_in.trim.split("_")(1).toInt

    val localpath = args(4)
    val run_style = args(5)
    val algo_sele = args(6)
    val result_save = args(7)

    val conf = new SparkConf().setAppName("WTA")
    if("local".equals(run_style.trim)){
      if(task_num>0) {
        conf.setMaster(s"local[$task_num]") //local run
      }else{
        conf.setMaster("local[1]")//local run
      }
    }

    val sc = new SparkContext(conf)
    //ready to record
    val record = new Record()
    //read data
    val (rawAVS, rawDSAK, rawSANG) = new ReadData(localpath.trim).apply(sc, stagenum)
    rawAVS.cache()
    rawDSAK.cache()
    rawSANG.cache()
    //begin
    val starttime = new Date().getTime
    val (dsak_j, avs, sang) = deal_Jup(stagenum, sc, rawAVS, rawDSAK, rawSANG)
    //run
    val J_max = dsak_j.map(dsak_j => dsak_j.Jup).max
    //global pher and probability
    val g_Pher: Array[Array[Double]] = Array.ofDim(stagenum, J_max + 1)
    g_Pher.map(_ => pher_max)
    algo_sele.trim match {
      case "basic" => {
        //init an ant, add in bestants
        val bestants = scala.collection.mutable.ArrayBuffer[T_Ant](
          new Ant(g_Pher, stagenum, J_max, dsak_j, avs, sang, "basic")
        )
        Basic_run(stagenum, iter_num, ant_num, bestants, J_max, dsak_j,
          avs, sang, sc, record)
        //end
        val stoptime = new Date().getTime
        record.time_run = stoptime-starttime
        //output
        val outputs = Output(stagenum, iter_num, ant_num, task_num,
          localpath, run_style, algo_sele, result_save,
          record, bestants).sortWith(_.ant.Fobj>_.ant.Fobj)
        result_save match {
          case "local" =>{
            Util.saveToLocal(outputs)
          }
          case _ => {
            val hdfs_path = "hdfs://192.168.120.133:8020/WTA/data/output/"
            sc.parallelize(outputs,1).saveAsTextFile(hdfs_path)
          }
        }
      }

      case _ => {
        //init an ant, add in bestants
        val bestants = scala.collection.mutable.ArrayBuffer[T_Ant](
          new Ant(g_Pher, stagenum, J_max, dsak_j, avs, sang, "distri")
        )
        //broadcast
        val broad_dsak_j = sc.broadcast(dsak_j)
        val broad_avs = sc.broadcast(avs)
        val broad_sang = sc.broadcast(sang)
        val broad_bestants =
          BroadcastWrapper[scala.collection.mutable.ArrayBuffer[T_Ant]](sc,
            bestants)
        Distri_run(stagenum, iter_num, ant_num, broad_bestants, J_max,
          broad_dsak_j, broad_avs, broad_sang, sc, task_num, record)
        //end
        val stoptime = new Date().getTime
        record.time_run = stoptime-starttime
        //output
        val outputs = Output(stagenum, iter_num, ant_num, task_num,
          localpath, run_style, algo_sele, result_save,
          record, broad_bestants.value).sortWith(_.ant.Fobj>_.ant.Fobj)
        result_save match {
          case "local" =>{
            Util.saveToLocal(outputs)
          }
          case _ => {
            val hdfs_path = "hdfs://192.168.120.133:8020/WTA/data/output/"
            sc.parallelize(outputs,1).saveAsTextFile(hdfs_path)
          }
        }
      }
    }

    sc.stop()
  }
}
