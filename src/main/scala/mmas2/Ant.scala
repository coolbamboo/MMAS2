package mmas2

import java.util.concurrent.ThreadLocalRandom
import mmas2.Para._
import mmas2.Util.randval

import java.util.Date
import scala.math.pow
import scala.util.Random

/**
  * ant used after deal_U
  * use ant：
  * search for decision variable
  * compute object fun
  *
  * stagenum:decision param's num(12\180)
  */
class Ant(init_Pher: Array[Array[Double]], val stagenum: Int, val Jmax: Int,
          vdsak_j: Array[DSAK_Jup], rawAVS: Array[AVS], rawSANG: Array[SANG], algoSele:String
         ) extends Serializable with T_Ant {

  val avss: Array[AVS] = rawAVS
  val sangs: Array[SANG] = rawSANG
  val dsaks: Array[DSAK_Jup] = vdsak_j
  var rand : Random = _

  /**
    * variable in stage length(the last non-zero row num)
    */
  override var pathlength = 0
  val local_step = 1 //optimal added incremental

  val prob: Array[Array[Double]] = Array.ofDim(stagenum, Jmax + 1)
  //init（catch global pher）
  override val pher: Array[Array[Double]] = init_Pher.clone()
  //decision variable
  override val Xdsa: Array[Int] = new Array(stagenum)
  //object fun
  override var Fobj: Double = 0.0
  //D(defend weapon amount)
  override var b_s: Array[Double] = new Array(D(stagenum))
  //Ground
  override var g_s:Array[Double] = new Array(S(stagenum))
  //C
  override var c_s:Array[Double] = new Array(D(stagenum))
  //Manpower
  override var m_s:Array[Double] = new Array(D(stagenum))

  def setRandom(rand : Random) : Unit = {
    this.rand = rand
  }

  //search once
  def search(): Unit = {
    dsaks.foreach {
      case DSAK_Jup(num, d, s, a, kdsa, jup) =>
        //deal every stage
        //choose which j?
        val l = constraints(num) //j's up bound
        if (l == 0) { //if l is 0, after l 's variable are all 0
          pathlength = num - 1
          //no use to compute
          return
        } else {
          //choose which l?
          val probl: Array[Double] = new Array[Double](l + 1)
          //this row:num to random variable xdsa
          Xdsa(num - 1) = computeProb_l(num, l, probl)
        }
        pathlength = num //row num
    }
  }
  //to some stage i, judge if match constraint before i, if true, return max j
  private def constraints(row:Int): Int={
    //compute constraint
    val B_total:Array[Int] = new Array(B(stagenum).length)
    val G_total:Array[Int] = new Array(S(stagenum))
    var C_total = 0
    var M_total:Array[Int] =new Array(M(stagenum).length)
    val cst_dsak_j = dsaks.filter(x=>x.num<row).map(dsak => {
      val d=dsak.d
      val s=dsak.s
      B_total(d-1) = B_total(d-1) + Xdsa(dsak.num-1)
      G_total(s-1) = G_total(s-1) + Xdsa(dsak.num-1)*t(stagenum)(d-1)
      C_total = C_total + Xdsa(dsak.num-1)*c(stagenum)(d-1)
      M_total(d-1) = M_total(d-1)+Xdsa(dsak.num-1)*m(stagenum)(d-1)
    }
    )
    //befor row-1 variable is done, compute this row 's max j
    var l:Int = 0
    dsaks.filter(x=>x.num==row).foreach{ dsak => {
      val d=dsak.d
      val s=dsak.s
      var gs:Array[Int] = avss.filter(x=>x.s==s).map(x=>x.Gs)
      l = Array(B(stagenum)(d-1)-B_total(d-1),
        (gs(0)-G_total(s-1))/t(stagenum)(d-1),
        (Cmax(stagenum)-C_total)/c(stagenum)(d-1),
        (M(stagenum)(d-1)-M_total(d-1))/m(stagenum)(d-1)
      ).min
    }
    }
    if(l<0) throw new Exception("l compute wrong！")
    l
  }

  private def computeProb_l(num: Int, l : Int, probl : Array[Double]): Int ={
    //from 0 to l's trans prob
    var total_g_pher = 0.0
    for(i<-0 to l) {
      total_g_pher = total_g_pher + pher(num - 1)(i)
    }
    var selectedL = 0
    //transform prob
    if(total_g_pher > 0) {
      for (j <- 0 to l) {
        probl(j) = pher(num - 1)(j) / total_g_pher
        prob(num - 1)(j) = probl(j)
      }
      //choose
      //val random : ThreadLocalRandom = ThreadLocalRandom.current()
      //random.setSeed(Thread.currentThread().getId)
      //var temp: Double = random.nextDouble(0, 1.0) //0~1.0之间

      //var temp: Double = rand.nextDouble()
      var temp: Double = 0.0
      algoSele match {
        case "basic" => temp = randval(0,1)
        case _ =>
          temp = rand.nextDouble()
          //Thread.sleep(delay_milisec)
          //temp = randval(0,1)//配合sleep，效果不显
          //println(temp)
      }

      import scala.util.control.Breaks._
      breakable {
        for (i <- 0 to l) {
          temp = temp - probl(i)
          if (temp < 0.0) {
            selectedL = i
            break()
          }
        }
      }
    }else{//choose first, is 0
      selectedL = 0
    }
    selectedL
  }

  //Whether the whole decision variable Xdsa match constraints
  private def matchconstraints_all_xdsa(pathL : Int):Boolean={
    val B_total:Array[Int] = new Array(B(stagenum).length)
    val G_total:Array[Int] = new Array(S(stagenum))
    var C_total = 0
    val M_total: Array[Int] = new Array(M(stagenum).length)
    dsaks.filter(x=>x.num <= pathL).foreach(dsak=>{
      val d=dsak.d
      val s=dsak.s
      B_total(d-1) = B_total(d-1) + Xdsa(dsak.num-1)
      G_total(s-1) = G_total(s-1) + Xdsa(dsak.num-1)*t(stagenum)(d-1)
      C_total = C_total + Xdsa(dsak.num-1)*c(stagenum)(d-1)
      M_total(d-1) = M_total(d-1)+Xdsa(dsak.num-1)*m(stagenum)(d-1)
    }
    )
    //match constraints?
    val gs:Array[Int] = avss.map(x=>x.Gs)
    if(!Util.morethan(B_total, B(stagenum)) && !Util.morethan(M_total, M(stagenum))
    && C_total<= Cmax(stagenum) && !Util.morethan(G_total, gs)){
      true//满足约束
    }else{
      false
    }
  }

  private def local_optimization():Unit = {
    //to random one decision variable Xdsa, add step,if match constraint,success
    val i = ThreadLocalRandom.current().nextInt(stagenum)
    Xdsa(i) = Xdsa(i) + local_step
    if(!matchconstraints_all_xdsa(pathlength)) Xdsa(i) = Xdsa(i) - local_step
  }

  private def computeObjectFunction(): Unit ={
    var f = 0.0
    avss.foreach(avs=>{
      val s = avs.s
      val vs = avs.v
      //attacker 's TT
      var attack:Double = 1.0
      sangs.filter(x=>x.s==s).foreach(sang => {
        val a = sang.a
        val nsa = sang.nsa
        val gsa = sang.gsa
        //defense:（1-kdsa）xdsa/nsa TT
        var defense:Double = 1.0
        dsaks.filter(dsak=>dsak.s==s && dsak.a==a).foreach{ dsak1 => {
          val num = dsak1.num
          val kdsa = dsak1.Kdsa
          defense = defense * pow(1.0 - kdsa, Xdsa(num - 1) / nsa.toDouble)
        }
        }
        attack = attack*pow(1.0-defense*gsa,nsa)
      })
      f = f + vs * attack
    })
    Fobj = f / avss.map(_.v).sum
  }

  private def constraintShow(): Unit ={
    //D
    for(i <- b_s.indices){
      b_s(i) = dsaks.filter(_.d==i+1).map(dsak=>Xdsa(dsak.num-1)).sum
    }
    //C
    for(i <- c_s.indices){
      c_s(i) = dsaks.filter(_.d==i+1).map(dsak=>c(stagenum)(i)*Xdsa(dsak.num-1)).sum
    }
    //M
    for(i <- m_s.indices){
      m_s(i) = dsaks.filter(_.d==i+1).map(dsak=>m(stagenum)(i)*Xdsa(dsak.num-1)).sum
    }
    //G
    for(i <- g_s.indices){
      g_s(i) = dsaks.filter(_.s==i+1).map(dsak=>t(stagenum)(dsak.d-1)*Xdsa(dsak.num-1)).sum
    }
  }

  override def dealflow(): Unit ={
    //
    search()
    local_optimization()
    //
    computeObjectFunction()
    //
    constraintShow()
  }

}