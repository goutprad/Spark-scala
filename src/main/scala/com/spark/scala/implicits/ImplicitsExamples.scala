package com.spark.scala.implicits

object ImplicitsExamples extends App {
  //Case 1: implicit parameters
  implicit val gout = "Goutam"

  def greet(implicit name: String) = {
    println(s"Hello $name")
  }

  greet //calling the above method

  //Example 1 (If two implicit in same scope)
  //implicit val prad = "Pradhan"

  //greet    //here it will thorugh error as we have two implicit variable, its ambiguous

  //Case 2: Type conversions with implicit functions
  implicit def intToStr(num: Int): String = s"The value is $num"
  val str = 42.toUpperCase()
  println(str)

  def functionTakingString(str: String) = str
  // note that we're passing int
  val str1 = functionTakingString(42)
  println(str1) // evaluates to "The value is 42"

  //Case 3: “Pimp my library”
  implicit def stringToStringOps(str: String): StringOps = StringOps(str)

  println("Hello world".yell) // evaluates to "HELLO WORLD!"
  println("How are you?".isQuestion) // evaluates to 'true'

  //Case 4: Type classes
  implicit object IntegerMonoid extends Monoid[Int] {
    override def zero: Int = 0
    override def plus(a: Int, b: Int): Int = a + b
  }
  implicit object StringMonoid extends Monoid[String] {
    override def zero: String = ""
    override def plus(a: String, b: String): String = a.concat(b)
  }
  def sum[A](values: Seq[A])(implicit ev: Monoid[A]): A = values.foldLeft(ev.zero)(ev.plus)

  val res = sum(Seq(1, 2, 3, 4))
  println(res)

  val res1 = sum(Seq("A", "B", "C"))
  println(res1)
}
case class StringOps(str: String) {
  def yell = str.toUpperCase() + "!"
  def isQuestion = str.endsWith("?")
}

trait Monoid[A] {
  def zero: A
  def plus(a: A, b: A): A
}