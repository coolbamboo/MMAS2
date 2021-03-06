package mmas2

import scala.collection.mutable.ArrayBuffer

/**
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
class Output(stagenum: Int, iter_num: Int, ant_num: Int, task_num: Int,
             dataPath: String, runStyle: String, algoSele: String,
             record: Record, val ant: T_Ant) extends Serializable {

  override def toString: String = {
    val sb = new StringBuilder("")
    sb.append(s"当前是第${record.now_run}次运行,")
    sb.append(stagenum).append(",")
    sb.append(iter_num).append(",")
    sb.append(ant_num).append(",")
    sb.append(task_num).append(",")
    sb.append(dataPath).append(",")
    sb.append(runStyle).append(",")
    sb.append(algoSele).append(",")

    sb.append(record.time_run).append(",")
    sb.append(record.time_everyiter).append(",")
    sb.append(ant.Fobj).append(",")
    sb.append(ant.b_s.toSeq.toString().replace(",", "-")).append(":X,")
    sb.append(ant.g_s.toSeq.toString().replace(",", "-")).append(":ground,")
    sb.append(ant.c_s.sum).append(":costs,")
    sb.append(ant.m_s.toSeq.toString().replace(",", "-")).append(":manpower,")
    for (i <- ant.Xdsa.indices) {
      if (ant.Xdsa(i) > 0)
        sb.append("(").append(i + 1).append("_").append(ant.Xdsa(i)).append(")")
    }
    //sb.append("\r\n")
    sb.toString()
  }

}

object Output {

  def apply(stagenum: Int, iter_num: Int, ant_num: Int, task_num: Int,
            dataPath: String, runStyle: String, algoSele: String,
            record: Record,
            bestAnts: ArrayBuffer[T_Ant]): Vector[Output] = {
    bestAnts.map(x => new Output(stagenum, iter_num, ant_num, task_num,
      dataPath, runStyle, algoSele, record, x)).toVector
  }
}
