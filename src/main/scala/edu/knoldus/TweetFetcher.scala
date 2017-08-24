package edu.knoldus

import edu.knoldus.Models.UserToHashTagList
import twitter4j.TwitterFactory
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Neelaksh on 21/8/17.
  */
class TweetFetcher(twitterFactory: TwitterFactory) {

  private val twitterObj = twitterFactory.getInstance

  /**
    * @param userName the term according to which tweets are fetched
    * @return list of the latest 20 tweets and name of the person fetched by userName
    */
  def getUniqueHashTags(userName: String): Future[UserToHashTagList] = {
    Future {
      val result = twitterObj.getUserTimeline(userName)
      val nameAndTweetList =
        for {tweet <- result.asScala.toList
        } yield tweet.getHashtagEntities.toList
      val data = nameAndTweetList.flatten.map(x => x.getText).distinct
      UserToHashTagList(userName, data)
    }
  }
}
