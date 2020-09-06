package com.spark.scala.dataframe.joins

import org.apache.spark.sql.SparkSession

object JoinExample extends App {
  val spark = SparkSession.builder().appName("Joins Example").master("local[1]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val empColumns = Seq("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
  val deptColumns = Seq("deptno", "dname", "loc")

  val emp = Seq(
    (7369, "SMITH", "CLERK", "7902", "1980-12-17", "800.00", null, "20"),
    (7499, "ALLEN", "SALESMAN", "7698", "1981-02-20", "1600.00", "300.00", "30"),
    (7521, "WARD", "SALESMAN", "7698", "1981-02-22", "1250.00", "500.00", "30"),
    (7566, "JONES", "MANAGER", "7839", "1981-04-02", "2975.00", null, "20"),
    (7654, "MARTIN", "SALESMAN", "7698", "1981-09-28", "1250.00", "1400.00", "30"),
    (7698, "BLAKE", "MANAGER", "7839", "1981-05-01", "2850.00", null, "30"),
    (7782, "CLARK", "MANAGER", "7839", "1981-06-09", "2450.00", null, "10"),
    (7788, "SCOTT", "ANALYST", "7566", "1982-12-09", "3000.00", null, "20"),
    (7839, "KING", "PRESIDENT", null, "1981-11-17", "5000.00", null, "10"),
    (7844, "TURNER", "SALESMAN", "7698", "1981-09-08", "1500.00", "0.00", "30"),
    (7876, "ADAMS", "CLERK", "7788", "1983-01-12", "1100.00", null, "20"),
    (7900, "JAMES", "CLERK", "7698", "1981-12-03", "950.00", null, "30"),
    (7902, "FORD", "ANALYST", "7566", "1981-12-03", "3000.00", null, "20"),
    (7934, "MILLER", "CLERK", "7782", "1982-01-23", "1300.00", null, "10"))

  val dept = Seq(
    ("10", "ACCOUNTING", "NEW YORK"),
    ("20", "RESEARCH", "DALLAS"),
    ("30", "SALES", "CHICAGO"),
    ("40", "OPERATIONS", "BOSTON"))

  import spark.implicits._
  val empDF = emp.toDF(empColumns: _*)
  val deptDF = dept.toDF(deptColumns: _*)
  empDF.show()
  deptDF.show()
  
  println("========= Inner Join =========")
  val innerJoinDF = empDF.join(deptDF,Seq("deptno"),"inner")
  innerJoinDF.show()  
  
   println("========= Outer Join =========")
  empDF.join(deptDF,Seq("deptno"),"outer")
    .show(false)
    
  println("========= Full Join =========")
  empDF.join(deptDF,Seq("deptno"),"full")
    .show(false)
    
  println("========= Fullouter Join =========")
  empDF.join(deptDF,Seq("deptno"),"fullouter")
    .show(false)

  println("========= Right Join =========")
  empDF.join(deptDF,Seq("deptno"),"right")
    .show(false)
    
  println("========= Rightouter Join =========")
  empDF.join(deptDF,Seq("deptno"),"rightouter")
    .show(false)

  println("========= Left Join =========")
  empDF.join(deptDF,Seq("deptno"),"left")
    .show(false)
    
  println("========= Leftouter Join =========")
  empDF.join(deptDF,Seq("deptno"),"leftouter")
    .show(false)

  println("========= Leftanti Join =========")
  empDF.join(deptDF,Seq("deptno"),"leftanti")
    .show(false)

  println("========= Leftsemi join =========")
  empDF.join(deptDF,Seq("deptno"),"leftsemi")
    .show(false)

  println("========= Cross join =========")
  empDF.join(deptDF,empDF("deptno") === deptDF("deptno"),"cross")
    .show(false)

  println("========= crossJoin() =========")
  empDF.crossJoin(deptDF).show(false)
  
}