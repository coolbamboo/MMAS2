package mmas2


import java.util.Date

import MMAS.{AntIteration, Output}
import mmas2.common._

import org.apache.spark.{SparkConf, SparkContext}

//append dfs
/**
 * Created by root on 2016/3/6.
  * imported parameter:
  * args0: stage num(variable num)
  * args1: iteration num
  * args2: ant num(every iteration)
  * args3: par_3(whether partition and how many pars,"nopar" is use no concurrency)
 */
object main {
  def main(args: Array[String]) {
    val stagenum:Int = args(0).toInt
    val iter_num:Int = args(1).toInt
    val ant_num:Int = args(2).toInt
    val par_in = args(3)

    val conf = new SparkConf().setAppName("WTA")

    var is_par = ""
    var par_num = 0
    if(!"nopar".equals(par_in.trim)){
      is_par = par_in.trim.split("_")(0)
      par_num = par_in.trim.split("_")(1).toInt
      //conf.setMaster(s"local[$par_num]")//local run
    }else{
      conf.setMaster("local")//local run
    }

    val sc = new SparkContext(conf)
    val record = new Record()
    //read data
    val (rawAVS, rawDSAK, rawSANG) = ReadData(sc, stagenum)
    rawAVS.cache()
    rawDSAK.cache()
    rawSANG.cache()
    //begin
    val starttime = new Date().getTime
    val (dsak_j, avs, sang) = deal_Jup(stagenum, sc, rawAVS, rawDSAK, rawSANG)
    //broadcast
    val broad_dsak_j = sc.broadcast(dsak_j)
    val broad_avs = sc.broadcast(avs)
    val broad_sang = sc.broadcast(sang)

    val J_max = broad_dsak_j.value.map(dsak_j => dsak_j.Jup).max
    //global pher and probability
    val g_Pher: Array[Array[Double]] = Array.ofDim(stagenum, J_max + 1)
    val g_Prob: Array[Array[Double]] = Array.ofDim(stagenum, J_max + 1)
    //init pher = pherMax
    for (i <- 0 until stagenum)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = pher_max
      }
    //init an ant, add in bestants
    val bestants = scala.collection.mutable.ArrayBuffer[T_Ant](
      new Ant(g_Pher, stagenum, J_max, broad_dsak_j.value, broad_avs.value
        , broad_sang.value)
    )
    //run
    AntIteration(stagenum, iter_num, ant_num, bestants, J_max, broad_dsak_j.value,
      broad_avs.value, broad_sang.value, sc, par_num, is_par, record)
    //end
    val stoptime = new Date().getTime
    record.time_run = stoptime-starttime

    //output
    val outputs = Output(stagenum, iter_num, ant_num, is_par, par_num,
      record, bestants).sortWith(_.ant.Fobj>_.ant.Fobj)
    val hdfs_path = "hdfs://192.168.120.133:8020/WTA/data/output/"
    sc.parallelize(outputs,1).saveAsTextFile(hdfs_path)
    /*import java.io.{ByteArrayInputStream, IOException}
    import java.net.URI
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.apache.hadoop.io.IOUtils
    val hadoopconf = new Configuration()
    hadoopconf.setBoolean("dfs.support.append", true)
    try {
      val fs = FileSystem.get(URI.create(hdfs_path), hadoopconf)
      //append output object
      val in = new ByteArrayInputStream(outputs.map(_.toString()).reduce(_+_)
        .getBytes)
      val out = fs.append(new Path(hdfs_path))
      IOUtils.copyBytes(in, out, 4096, true)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }*/
    sc.stop()
  }
}
