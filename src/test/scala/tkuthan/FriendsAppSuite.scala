/* Copyright (c) 2019 Tomas Kuthan */
package tkuthan

import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions
import org.junit.Test

/**
  * @todo make and test private methods - PrivateMethodTester
  */

class FriendsAppSuite extends Assertions {

  val spark: SparkSession = SparkSession.builder()
    .appName("friends-unit-tests")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  @Test def shouldParseWithTwoFriends(): Unit = {
    val str = "alice, 50, bob, carol"
    val person = PersonString.unapply(str)
    assert(person.isDefined)
    assert(person.get.name === "alice")
    assert(person.get.money === 50)
    assert(person.get.friends === Array("bob", "carol"))
  }

  @Test def shouldParseWithOneFriend(): Unit = {
    val str = "alice,50,bob"
    val person = PersonString.unapply(str)
    assert(person.isDefined)
    assert(person.get.name === "alice")
    assert(person.get.money === 50)
    assert(person.get.friends === Array("bob"))
  }

  @Test def shouldParseWithoutFriends(): Unit = {
    val str = "alice, 50  "
    val person = PersonString.unapply(str)
    assert(person.isDefined)
    assert(person.get.name === "alice")
    assert(person.get.money === 50)
    assert(person.get.friends === Array())
  }

  @Test def shouldNotParseWithoutMoney(): Unit = {
    val str = "alice"
    val person = PersonString.unapply(str)
    assert(person.isEmpty)
  }

  @Test def shouldNotParseOnInvalidMoney(): Unit = {
    val str = "alice, InvalidMoney"
    val person = PersonString.unapply(str)
    assert(person.isEmpty)
  }

  @Test def personWithTwoFriendsShouldContributeTwoFriendNamePairs(): Unit = {
    val people = Seq(Person("alice", 50, Array("bob", "carol"))).toDS()
    val underTest = FriendsApp.extractFriendNamePairs(people)
    assert(underTest.count() === 2)
    assert(underTest.collect() === Array(FriendName("bob", "alice"), FriendName("carol", "alice")))
  }

  @Test def personWithoutFriendsShouldNotContributeFriendNamePairs(): Unit = {
    val people = Seq(Person("alice", 50, Array())).toDS()
    val underTest = FriendsApp.extractFriendNamePairs(people)
    assert(underTest.count() === 0)
  }

  @Test def friendsShouldBeMappedToTheirMoneyWorth(): Unit = {
    val nm = Seq(NameMoney("alice", 50L)).toDS()
    val fn = Seq(FriendName("alice", "bob")).toDS()
    val underTest = FriendsApp.mapFriendsToMoney(nm, fn)
    assert(underTest.count === 1)
    assert(underTest.collect() === Array(NameMoney("bob", 50L)))
  }

  @Test def friendsWithoutMoneyShouldNotBeConsidered(): Unit = {
    val nm = spark.emptyDataset[NameMoney]
    val fn = Seq(FriendName("alice", "bob")).toDS()
    val underTest = FriendsApp.mapFriendsToMoney(nm, fn)
    assert(underTest.count === 0)
  }

  @Test def friendsMoneyShouldAddUp(): Unit = {
    val nm = Seq(
      NameMoney("alice", 50L),
      NameMoney("alice", 25L),
      NameMoney("bob", 100L)).toDS()
    val underTest = FriendsApp.countFriendsMoney(nm)
    assert(underTest.count() === 2)
    assert(underTest.collect === Array(NameMoney("alice", 75L), NameMoney("bob", 100L)))
  }

  @Test def shouldBeAbleToGetSinglePersonWithRichestFriends(): Unit = {
    val nm = Seq(NameMoney("alice", 50L), NameMoney("bob", 25L), NameMoney("carol", 100L)).toDS
    val underTest = FriendsApp.getPeopleWithRichestFriends(nm)
    assert(underTest.length === 1)
    assert(underTest(0) === "carol")
  }

  @Test def shouldBeAbleToGetMultiplePeopleWithRichestFriends(): Unit = {
    val nm = Seq(NameMoney("alice", 100L), NameMoney("bob", 25L), NameMoney("carol", 100L)).toDS
    val underTest = FriendsApp.getPeopleWithRichestFriends(nm)
    assert(underTest.length === 2)
    assert(underTest === Array("alice", "carol"))
  }

  @Test def shouldWorkEndToEnd(): Unit = {
    val people = Seq(
      Person("david", 10L, Array("petr", "josef", "andrea")),
      Person("andrea", 50L, Array("josef", "martin")),
      Person("petr", 20L, Array("david", "josef")),
      Person("josef", 5L, Array("andrea", "petr", "david")),
      Person("martin", 100L, Array("josef", "andrea", "david")
      )).toDS()
    val underTest = FriendsApp.peopleWithRichestFriends(people)
    assert(underTest.length === 1)
    assert(underTest(0) === "andrea")
  }
}



