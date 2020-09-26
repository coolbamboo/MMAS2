package mmas2

/**
  * interface for ant
  */
trait T_Ant {

  var pathlength:Int

  var Fobj:Double

  val pher:Array[Array[Double]]

  val Xdsa :Array[Int]

  var b_s: Array[Double]
  //Ground
  var g_s:Array[Double]
  //C
  var c_s:Array[Double]
  //Manpower
  var m_s:Array[Double]

  def dealflow()
}
