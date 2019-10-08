package com.franksen.hello

import com.franksen.one.PackageExe1
import com.franksen.two.PackageExe2

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-08-下午7:59
  */
object TestPackage {


  /**
    * 包和类可以不在同一路径下
    * 包和class文件一定在同一路径下
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val pack1 = new PackageExe1
    val pack2 = new PackageExe2

    println(pack1 + " " + pack2)
  }

}
