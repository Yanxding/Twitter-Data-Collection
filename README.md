# Twitter-Data-Collection
Although the Twitter API provides easy access to Twitter data, there are substantial limits on data that is able to be collected. This script combines Twitter API and webdriver to collect full conversation data on Twitter around a specified user in specified time period. Te collected content includes any tweets sent by or to or mention the specified user as well as any responses and retweets. <br />
The returning data contains following contents,

| __Columns__          | __Description__                         |
| 'key'                | Identifier of interaction chain         |
| 'tweet_id'           | ID of the tweet                         |
| 'time'               | Time of the tweet                       |
| 'text'               | Content of the tweet                    |
| 'auther_id'          | ID of the auther of the tweet           |
| 'auther_name'        | Auther name                             |
| 'reply_to_id'        | ID of the tweet to which this reply to  |
| 'reply_to_user_id'   | ID of the user that is replied to       | 
| 'reply_to_user_name' | Name of the user that is replied to     |
| 'retweet_ct'         | Number of retweets                      |
| 'favorite_ct'        | Number of favorites                     |
| 'follower_ct'        | Number of followers of the auther       |
| 'location'           | Location of the tweet being sent        |
| 'source'             | Source of the tweet                     |
| 'is_retweet'         | True if this is a retweet               |
