package mmas2

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 2016/3/7.
 * results:
  * stagenum
  * iter_num
  * ant_num
  * task_num
  * localpath,data file is sourcing from local(fileurl) or "hdfs"
  * "local" running , or other running style
  * basic run MMAS, or improved(distributed) run MMAS
  * result:"local" or save on "hdfs"
  * record
  * ant(func,...)
 */
class Output(stagenum:Int, iter_num:Int, ant_num:Int, task_num:Int,
             localpath: String, runstyle: String, algo_sele : String,
             resultsave : String, record:Record, val ant:T_Ant) extends Serializable{

  override def toString(): String ={
    val sb = new StringBuilder("")
    sb.append(stagenum).append(",")
    sb.append(iter_num).append(",")
    sb.append(ant_num).append(",")
    sb.append(task_num).append(",")
    sb.append(localpath).append(",")
    sb.append(runstyle).append(",")
    sb.append(algo_sele).append(",")
    sb.append(resultsave).append(",")
    sb.append(record.time_run).append(",")
    sb.append(record.time_everyiter).append(",")
    sb.append(ant.Fobj).append(",")
    sb.append(ant.b_s.toSeq.toString().replace(",","-")).append(":X,")
    sb.append(ant.g_s.toSeq.toString().replace(",","-")).append(":ground,")
    sb.append(ant.c_s.sum).append(":costs,")
    sb.append(ant.m_s.toSeq.toString().replace(",","-")).append(":manpower,")
    for(i <- 0 until ant.Xdsa.length){
      if(ant.Xdsa(i) >0)
        sb.append("(").append(i+1).append("_").append(ant.Xdsa(i)).append(")")
    }
    //sb.append("\r\n")
    sb.toString()
  }

}

object Output{

  def apply(stagenum:Int, iter_num:Int, ant_num:Int, task_num:Int,
            localpath:String, runstyle:String, algo_sele:String,
            resultsave:String, record:Record,
            bestAnts:ArrayBuffer[T_Ant]): Vector[Output] = {
    bestAnts.map(x=> new Output(stagenum, iter_num, ant_num, task_num,
      localpath, runstyle, algo_sele, resultsave, record, x)).toVector
  }
}
