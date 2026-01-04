module Database.PostgreSQL.Stakhanov.Metrics
 (

 -- * Get queue(s) metrics
   metrics
 , allMetrics

 -- * Queue metric getters
 , getQueueLength
 , getNewestMsgAge
 , getOldestMsgAge
 , getScrapeTime
 , getTotalMessages
 , getQueueVisibleLength

 ) where

import           Data.Int
import           Data.Time
import qualified Data.Vector                              as V
import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Session                            as S

-- | Get `Queue`'s `Metrics`.
--
-- > λ: metrics co MyQueue
-- > Right (Queue {queueName = "MyQueue", queueMetrics = Just (Metrics {queueLength = 4, newestMsgAge = 272336, oldestMsgAge = 798677, totalMessages = 4, scrapeTime = 2025-12-18 14:23:41.714705 UTC, queueVisibleLength = 4}), queueDetails = Nothing})
--
metrics
  :: Queue        -- ^ The name of the queue
  -> IO (Either S.SessionError Queue)
metrics q@Queue{..} =
  S.run (S.statement qName getMetrics) (unHasqlConn qPGConn) >>= pureMap addMetrics
  where
    addMetrics m = q { qMetrics = Just $ tupleToMetrics m }

-- | Get `Metrics` of all created `Queue`s
allMetrics
  :: Queue -- ^ The connection to PostgreSQL
  -> IO (Either S.SessionError (V.Vector Queue))
allMetrics Queue{..} =
  let c = unHasqlConn qPGConn
  in S.run (S.statement () getAllMetrics) c >>= pureMap (tupleToQueueWithMetrics c <$>)

-- | Number of messages currently in the queue.
getQueueLength :: Queue -> Maybe Int64
getQueueLength (Queue _ _ _  (Just Metrics{..})) = Just queueLength
getQueueLength (Queue _ _ _ Nothing)             = Nothing

-- | Age of the newest message in the queue, in seconds.
getNewestMsgAge :: Queue -> Maybe Seconds
getNewestMsgAge (Queue _ _ _ (Just Metrics{..})) = newestMsgAge
getNewestMsgAge (Queue _ _ _ Nothing)            = Nothing

-- | Age of the oldest message in the queue, in seconds.
getOldestMsgAge :: Queue -> Maybe Seconds
getOldestMsgAge (Queue _ _ _ (Just Metrics{..})) = oldestMsgAge
getOldestMsgAge (Queue _ _ _ Nothing)            = Nothing

-- | Total number of messages that have passed through the queue over all time.
getTotalMessages :: Queue -> Maybe Int64
getTotalMessages (Queue _ _ _ (Just Metrics{..})) = Just totalMessages
getTotalMessages (Queue _ _ _ Nothing)            = Nothing

-- | The current timestamp
getScrapeTime :: Queue -> Maybe UTCTime
getScrapeTime (Queue _ _ _ (Just Metrics{..})) = Just scrapeTime
getScrapeTime (Queue _ _ _ Nothing)            = Nothing

-- | Number of messages currently visible (vt <= now).
getQueueVisibleLength :: Queue -> Maybe Int64
getQueueVisibleLength (Queue _ _ _ (Just Metrics{..})) = Just queueVisibleLength
getQueueVisibleLength (Queue _ _ _ Nothing)            = Nothing

