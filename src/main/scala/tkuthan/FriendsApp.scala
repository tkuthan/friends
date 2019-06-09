/* Copyright (c) 2019 Tomas Kuthan */
package tkuthan

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

/**
  * @todo stress test
  * @todo consider broadcast hash join
  */
object FriendsApp {

  val spark: SparkSession = SparkSession.builder()
    .appName("friends")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]) {
    val people = readFriends(args(0))

    val luckyGuys = peopleWithRichestFriends(people)

    println("-----------------------------------")
    luckyGuys.foreach(println(_))
    println("-----------------------------------")
  }

  private def readFriends(filename: String) = {
    spark.read.text(filename)
      .map(_.getString(0))
      .map(str => PersonString.unapply(str).orNull)
      .filter(_ != null)
  }

  /**
    * Given a dataset of people, returns an array of one or more users with the wealthiest friends.
    *
    * @param people Dataset(People(name, money, Array(friends))
    * @return an array of one or more users with the wealthiest friends.
    */
  def peopleWithRichestFriends(people: Dataset[Person]): Array[String] = {
    val nm = people.select("name", "money").as[NameMoney]
    val fn = extractFriendNamePairs(people)
    val joined = mapFriendsToMoney(nm, fn)
    val summed = countFriendsMoney(joined)
    getPeopleWithRichestFriends(summed)
  }

  /**
    * Given a a dataset of people and sum of money of their friends,
    * returns an array of people with richest friends
    *
    * @param summed Dataset(NameMoney(name, sum_of_friends_money))
    * @return an array of people with maximal sum of friends money
    */
  def getPeopleWithRichestFriends(summed: Dataset[NameMoney]): Array[String] = {
    val mx = summed.groupBy().max("money").map(_.getLong(0)).first
    summed.filter(_.money >= mx).map(_.name).collect
  }

  /**
    * Sums money of friends
    *
    * @param joined Dataset(NameMoney(name, one_friends_money))
    * @return Dataset(NameMoney(name, sum_of_friends_money))
    */
  def countFriendsMoney(joined: Dataset[NameMoney]): Dataset[NameMoney] = {
    joined.groupBy('name).sum("money").withColumnRenamed("sum(money)", "money").as[NameMoney]
  }

  /**
    * Performs name -> money lookup
    *
    * @param nm Dataset(NameMoney(name, his_money))
    * @param fn Dataset(FriendName(one_friend, name))
    * @return Dataset(NameMoney(name, one_friends_money))
    */
  def mapFriendsToMoney(nm: Dataset[NameMoney], fn: Dataset[FriendName]): Dataset[NameMoney] = {
    fn.join(nm, $"friend" === $"name")
      .select("person", "money")
      .withColumnRenamed("person", "name")
      .as[NameMoney]
  }

  /**
    * Creates name-friend pairs from people
    *
    * @param people Dataset(People(name, money, Array(friends))
    * @return Dataset(FriendName(one_friend, name))
    */
  def extractFriendNamePairs(people: Dataset[Person]): Dataset[FriendName] = {
    people
      .flatMap(p => p.friends.map(f => (f, p.name)))
      .withColumnRenamed("_1", "friend")
      .withColumnRenamed("_2", "person")
      .as[FriendName]
  }

}

/**
  * Represent one person entry
  *
  * @param name    Name, unique accross the whole dataset
  * @param money   Money, non-negative Integer
  * @param friends Array of friends of the user
  */
case class Person(name: String, money: Long, friends: Array[String])

/**
  * Represents a name-money pair.
  *
  * @param name  Name
  * @param money Money
  */
case class NameMoney(name: String, money: Long)

/**
  * Represents a name-friend pair
  *
  * @param person a user
  * @param friend his friend
  */
case class FriendName(friend: String, person: String)

/**
  * Extractor object for parsing money from string.
  */
object AsInt {
  def unapply(s: String): Option[Long] = {
    try {
      Some(s.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }
}

/**
  * Extractor object for parsing a Person from a line of input
  */
object PersonString {
  val personRegex: Regex = "^([^,]*),* *([0-9]*),*(.*)".r

  def unapply(str: String): Option[Person] = str match {
    case PersonString.personRegex(name, AsInt(money), friends) => Some(Person(name, money, if (friends.trim.isEmpty) Array() else friends.split(",").map(_.trim)))
    case _ => None
  }
}
