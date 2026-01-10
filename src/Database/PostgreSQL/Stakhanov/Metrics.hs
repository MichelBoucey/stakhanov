module Database.PostgreSQL.Stakhanov.Metrics
 (

 -- * Get queue(s) metrics
   metrics
 , allMetrics

 -- * Queue metrics getters
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
-- > λ: metrics MyQueue
-- > Right (Queue {qName = "MyQueue", qPGConn = "a Hasql connection", qDetails = Nothing, qMetrics = Just (Metrics {queueLength = 24, newestMsgAge = Just 447278, oldestMsgAge = Just 2192954, totalMessages = 27, scrapeTime = 2026-01-09 19:53:59.503568 UTC, queueVisibleLength = 24})})
--
metrics
  :: Queue                            -- ^ The name of the queue
  -> IO (Either S.SessionError Queue) -- ^ The queue with metrics added
metrics q@Queue{..} =
  S.run (S.statement qName getMetrics) (unHasqlConn qPGConn) >>= pureMap addMetrics
  where
    addMetrics m = q { qMetrics = Just $ tupleToMetrics m }

-- | Get `Metrics` of all created `Queue`s.
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

-- | The current timestamp.
getScrapeTime :: Queue -> Maybe UTCTime
getScrapeTime (Queue _ _ _ (Just Metrics{..})) = Just scrapeTime
getScrapeTime (Queue _ _ _ Nothing)            = Nothing

-- | Number of messages currently visible (vt <= now).
getQueueVisibleLength :: Queue -> Maybe Int64
getQueueVisibleLength (Queue _ _ _ (Just Metrics{..})) = Just queueVisibleLength
getQueueVisibleLength (Queue _ _ _ Nothing)            = Nothing

