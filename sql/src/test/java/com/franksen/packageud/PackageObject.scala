
/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-08-下午9:11
  */
package com.wys{

  package object scala{

    var name = "Package Object."

    def packageObjectFun(): Unit ={
      println("package object scala")
    }

  }

  package scala{

    /**
      * 包对象的实现机制分析，其中包含的原理：
      *    包对象生成两个.class文件：package.class package$.class
      *
      * 当包中其他类访问包对象的属性和方法时，调到的顺序为：
      *    package$.MOUDLE$.name
      *    package$.MOUDLE$.packageObjectFun()
      */



    object MyPackage{
      def main(args: Array[String]): Unit = {

        name = "frankSen"
        println("name" + name)
        packageObjectFun()

      }
    }

  }

}
