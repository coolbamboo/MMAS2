package mmas2


import java.util.Date

import mmas2.Para._
import mmas2.Util.deal_Jup
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

//append dfs
/**
  * Created by root on 2016/3/6.
  * imported parameter:
  * args0: stage num(variable num)
  * args1: iteration num
  * args2: ant num(every iteration)
  * args3: task_num(whether partition and how many pars)
  * args4: dataPath,data file is sourcing from local(fileurl) or "hdfs"
  * args5: "local" running , or other running style
  * args6: basic run MMAS, or improved(distributed) run MMAS
  * e.x. 12 1000 40 4 C:\Users\caoming\Desktop\mmastable\ local basic
  */
object main {
  def main(args: Array[String]) {
    val stagenum: Int = args(0).toInt
    val iter_num: Int = args(1).toInt
    val ant_num: Int = args(2).toInt
    val task_num = args(3).toInt

    val dataPath = args(4)
    val runStyle = args(5)
    val algoSele = args(6)

    val conf = new SparkConf().setAppName("WTA")
    if (task_num > 0)
      conf.set("spark.default.parallelism", task_num.toString)
    else
      conf.set("spark.default.parallelism", "1")

    if ("local".equals(runStyle.trim)) {
      if (task_num > 0) {
        conf.setMaster(s"local[$task_num]") //local run
      } else {
        conf.setMaster("local[1]") //local run
      }
    }

    val sc = new SparkContext(conf)
    //ready to record
    val record = new Record()
    //read data
    val (rawAVS, rawDSAK, rawSANG) = new ReadData(dataPath.trim).apply(sc, stagenum)
    val avses = rawAVS.collect()
    val dsaks = rawDSAK.collect()
    val sang = rawSANG.collect()
    //begin
    val starttime = new Date().getTime
    val (dsak_j, avs) = deal_Jup(stagenum, avses, dsaks)
    //compute J max bound for Pher array
    val J_max = dsak_j.map(dsak_j => dsak_j.Jup).max
    //global pher and probability
    val g_Pher: Array[Array[Double]] = Array.ofDim(stagenum, J_max + 1)
    //init
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = pher_max
      }
    //init an ant, add in bestants
    val bestants = ArrayBuffer[T_Ant](
      new Ant(g_Pher, stagenum, J_max, dsak_j, avs, sang)
    )
    //init an accumulator
    val globalBestAnts = new AntAccumulator(best_result_num)
    sc.register(globalBestAnts, "globalBestAnts")

    algoSele.trim match {
      case "basic" => {
        Basic_run(iter_num, ant_num, bestants, record)
        //end
        val stoptime = new Date().getTime
        record.time_run = stoptime - starttime
      }

      case _ => {
        //must add here
        val modelAnt = new Ant(g_Pher, stagenum, J_max, dsak_j, avs, sang)
        Distri_run(iter_num, ant_num, modelAnt, globalBestAnts, sc, record)
        //end
        val stoptime = new Date().getTime
        record.time_run = stoptime - starttime
      }
    }

    //output
    var outputs: Vector[Output] = null
    if (globalBestAnts.isZero) {
      outputs = Output(stagenum, iter_num, ant_num, task_num,
        dataPath, runStyle, algoSele, record, bestants).sortWith(_.ant.Fobj > _.ant.Fobj)
    } else {
      outputs = Output(stagenum, iter_num, ant_num, task_num,
        dataPath, runStyle, algoSele, record, globalBestAnts.value).sortWith(_.ant.Fobj > _.ant.Fobj)
    }
    runStyle.trim match {
      case "local" => {
        Util.saveToLocal(outputs)
      }
      case _ => {
        val hdfs_path = "hdfs://192.168.120.133:8020/WTA/data/output/"
        sc.parallelize(outputs, 1).saveAsTextFile(hdfs_path)
      }
    }

    sc.stop()
  }
}
