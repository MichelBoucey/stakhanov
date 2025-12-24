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
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S

-- | Get `Queue`'s `Metrics`.
--
-- > λ: metrics co mq
-- > Right (Queue {queueName = "mq", queueMetrics = Just (Metrics {queueLength = 4, newestMsgAge = 272336, oldestMsgAge = 798677, totalMessages = 4, scrapeTime = 2025-12-18 14:23:41.714705 UTC, queueVisibleLength = 4})})
--
metrics
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The name of the queue
  -> IO (Either S.SessionError Queue)
metrics c q@Queue{..} =
  S.run (S.statement queueName getMetrics) c >>= \e -> pure $ addMetrics <$> e
   where
     addMetrics m = q { queueMetrics = Just $ tupleToMetrics m }

-- | Get `Metrics` of all created `Queue`s
allMetrics
  :: C.Connection -- ^ The connection to PostgreSQL
  -> IO (Either S.SessionError (V.Vector Queue))
allMetrics c =
  S.run (S.statement () getAllMetrics) c >>= \e -> pure $ (tupleToQueueWithMetrics <$>) <$> e

-- | Number of messages currently in the queue.
getQueueLength :: Queue -> Maybe Int64
getQueueLength (Queue _ (Just Metrics { .. })) = Just queueLength
getQueueLength (Queue _ Nothing)               = Nothing

-- | Age of the newest message in the queue, in seconds.
getNewestMsgAge :: Queue -> Maybe Int32
getNewestMsgAge (Queue _ (Just Metrics{..})) = newestMsgAge
getNewestMsgAge (Queue _ Nothing)            = Nothing

-- | Age of the oldest message in the queue, in seconds.
getOldestMsgAge :: Queue -> Maybe Int32
getOldestMsgAge (Queue _ (Just Metrics{..})) = oldestMsgAge
getOldestMsgAge (Queue _ Nothing)            = Nothing

-- | Total number of messages that have passed through the queue over all time.
getTotalMessages :: Queue -> Maybe Int64
getTotalMessages (Queue _ (Just Metrics{..})) = Just totalMessages
getTotalMessages (Queue _ Nothing)            = Nothing

-- | The current timestamp
getScrapeTime :: Queue -> Maybe UTCTime
getScrapeTime (Queue _ (Just Metrics{..})) = Just scrapeTime
getScrapeTime (Queue _ Nothing)            = Nothing

-- | Number of messages currently visible (vt <= now).
getQueueVisibleLength :: Queue -> Maybe Int64
getQueueVisibleLength (Queue _ (Just Metrics{..})) = Just queueVisibleLength
getQueueVisibleLength (Queue _ Nothing)            = Nothing

