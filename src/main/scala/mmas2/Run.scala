package mmas2

import mmas2.Para._

trait Run {
  val modelAnt: Ant
  var bestAnt: T_Ant

  def maxmincheck(g_Pher: Array[Array[Double]]): Unit = {
    for (i <- 0 until modelAnt.stagenum)
      for (j <- 0 to modelAnt.Jmax) {
        if (g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if (g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }
}