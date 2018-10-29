package mmas2

import org.apache.spark._
import org.apache.spark.rdd.RDD
import common._
/**
 * Created by root on 2016/3/5.
 */
case class AVS(s:Int, v:Int, Gs:Int)
//Jup is up bound of J
case class DSAK_Jup(num:Int,d:Int, s:Int, a:Int, Kdsa:Double, Jup:Int = 0)
case class SANG(s:Int, a:Int, nsa:Int, gsa:Double)

class ReadData(localpath:String="hdfs") extends Serializable{

  val path = "hdfs://192.168.120.133:8020/WTA/data/input/"
  val avs_file = Map(12->"svg.csv",180->"1.csv")
  val dsak_file = Map(12->"dsak.csv",180->"2.csv")
  val sang_file = Map(12->"sang.csv",180->"3.csv")

  private def parseAVS(line: String) = {
    val pieces = line.split(',')
    val s = pieces(0).toInt
    val v = pieces(1).toInt
    val Gs = pieces(2).toInt
    AVS(s, v, Gs)
  }

  private def parseDSAK(line: String) = {
    val pieces = line.split(',')
    val num = pieces(0).toInt
    val d = pieces(1).toInt
    val s = pieces(2).toInt
    val a = pieces(3).toInt
    val Kdsa = pieces(4).toDouble
    DSAK_Jup(num, d, s, a, Kdsa)
  }

  private def parseSANG(line: String) = {
    val pieces = line.split(',')
    val s = pieces(0).toInt
    val a = pieces(1).toInt
    val nsa = pieces(2).toInt
    val gsa = pieces(3).toDouble
    SANG(s, a, nsa, gsa)
  }

  def apply(sc : SparkContext, stagenum:Int) = {
    (dealAVS(sc,stagenum) , dealDSAK(sc,stagenum),  dealSANG(sc,stagenum))
  }

  private def dealAVS(sc : SparkContext,stagenum:Int) = {
    //1, svg
    var rawAVS: RDD[String] = null
    if(localpath.equals("hdfs")) {
      rawAVS = sc.textFile(path + avs_file(stagenum))
    }else{
      rawAVS = sc.textFile(localpath + avs_file(stagenum))
    }
    val pasedAVS = rawAVS.map(line => parseAVS(line))
    pasedAVS
  }

  private def dealDSAK(sc:SparkContext,stagenum:Int) = {
    //2, dsak
    var rawDSAK:RDD[String] = null
    if(localpath.equals("hdfs")) {
      rawDSAK = sc.textFile(path + dsak_file(stagenum))
    }else{
      rawDSAK = sc.textFile(localpath + dsak_file(stagenum))
    }
    val pasedDSAK = rawDSAK.map(line => parseDSAK(line))
    pasedDSAK
  }

  private def dealSANG(sc: SparkContext,stagenum:Int) = {
    //3, sang
    var rawSANG:RDD[String] = null
    if(localpath.equals("hdfs")){
      rawSANG = sc.textFile(path + sang_file(stagenum))
    }else{
      rawSANG = sc.textFile(localpath + sang_file(stagenum))
    }
    val pasedSANG = rawSANG.map(line => parseSANG(line))
    pasedSANG
  }
}

object deal_Jup{
//return DSAK_Jup's RDD
  def apply(stagenum:Int, sc:SparkContext, rawAVS:RDD[AVS], rawDSAK:RDD[DSAK_Jup], rawSANG:RDD[SANG])= {
  //get value,not iter a RDD using another RDD
    val avss = rawAVS.collect()//
    val dsaks = rawDSAK.collect()
    val Vdsak_j = dsaks.map{_ match {
        case DSAK_Jup(num,d,s,a,kdsa,j)=>{
          //Gs
          var gs = avss.filter(x=>x.s==s).map(x=>x.Gs)
          val jup = Array(
            B(stagenum)(d-1),gs(0)/t(stagenum)(d-1),Cmax(stagenum)/c(stagenum)(d-1),M(stagenum)(d-1)/m(stagenum)(d-1)
          ).min
          if(jup<0) throw new Exception("jup is wrong！")
          DSAK_Jup(num,d,s,a,kdsa,jup)
        }
      }
    }
    (Vdsak_j,avss,rawSANG.collect())
  }
}

