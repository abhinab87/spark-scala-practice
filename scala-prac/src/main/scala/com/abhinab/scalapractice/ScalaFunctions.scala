package com.abhinab.scalapractice

object ScalaFunctions {

  //Higher Order Function
  private def promotion(salaries: List[Double], promotionFunction: Double => Double): List[Double] = salaries.map(promotionFunction)

  def smallPromotion(salaries: List[Double]): List[Double] = promotion(salaries, salary => salary * 1.1)

  def greatPromotion(salaries: List[Double]): List[Double] = promotion(salaries, salary => salary * math.log(salary))

  def hugePromotion(salaries: List[Double]): List[Double] = promotion(salaries, salary => salary * salary)

  def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
    val schema = if (ssl) "https://" else "http://"
    (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
  }

  def main(args: Array[String]): Unit = {
    val pivotData = Seq(("A","Q1",2000),("B","Q1",1000),("A","Q2",2900),("C","Q2",2800),("D","Q3",2010),("c","Q3",2040),("A","Q4",2300),("C","Q4",2200),("E","Q4",2100),("B","Q4",2100))

    //GroupBy Example 2nd highest profit
    val sd = pivotData.groupBy(_._1).map(x => (x._1, x._2.map(_._3).sortWith(_ > _))).map { x =>
      val v = if(x._2.size == 1) x._2(0) else x._2(1)
      (x._1, v)
    }

    //Fold, FoldLeft, FoldRight
    val fooList = Foo("Hugh Jass", 25, 'male) ::
      Foo("Biggus Dickus", 43, 'male) ::
      Foo("Incontinentia Buttocks", 37, 'female) ::
      Nil

    val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
      val title = f.sex match {
        case 'male => "Mr."
        case 'female => "Ms."
      }
      z :+ s"$title ${f.name}, ${f.age}"
    }

    println(stringList)

    //reduceLeft, reduceRight
    val peeps = Vector("al", "hannah", "emily", "christina", "aleka")
    val longestString = peeps.reduceLeft((x,y) => if (x.length > y.length) x else y)
    val shortestString = peeps.reduceLeft((x,y) => if (x.length < y.length) x else y)

    //Difference between fold & reduce is fold will have a seed value.

    println(s"Longest String is $longestString & Shortest String is $shortestString")

    val salary = Seq(1000.00, 2000.00)
    val promotionOfEmp = promotion(salary.toList, salary => salary*2)

    println(promotionOfEmp)
    val domainName = "www.example.com"
    def getURL = urlBuilder(ssl=true, domainName)
    val endpoint = "users"
    val query = "id=1"
    val url = getURL(endpoint, query)
    println(url)
  }
}
