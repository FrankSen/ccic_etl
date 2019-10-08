package net.ccic.etl.launcher

import java.lang.reflect.Modifier

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-09-12-下午2:36
  */
private [etl] trait EtlApplication {

  def start(args: Array[String]): Unit

}


private[etl] class JavaMainApplication(klass: Class[_]) extends EtlApplication {

  override def start(args: Array[String]): Unit = {
    val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)){
      throw new IllegalStateException("The main method in the given main class must be static ")
    }

    mainMethod.invoke(null, args)

  }
}