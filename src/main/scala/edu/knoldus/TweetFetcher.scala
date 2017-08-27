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
    * @param userName screenname from whose profile tweets are feteched
    * @return an instance of UserTOHashTagList with all the unique hashtags
    *         and the screename of the user
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
