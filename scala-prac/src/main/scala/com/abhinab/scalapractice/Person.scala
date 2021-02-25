package com.abhinab.scalapractice

class Person{
  var name: Option[String] = None
  var age: Option[Int] = None
  override def toString = s"$name, $age"
}

/*
Advantages of Companion Object
  A companion object is an object that’s declared in the same file as a class, and has the same name as the class
  A companion object and its class can access each other’s private members
  A companion object’s apply method lets you create new instances of a class without using the new keyword
  A companion object’s unapply method lets you de-construct an instance of a class into its individual components
 */
object Person {
  def apply(name: Option[String]): Person = {
    var p = new Person
    p.name = name
    p
  }
  def apply(name: Option[String], age: Option[Int]): Person = {
    var p = new Person
    p.name = name
    p.age = age
    p
  }
  def unapply(p: Person): String = s"${p.name}, ${p.age}"
}
