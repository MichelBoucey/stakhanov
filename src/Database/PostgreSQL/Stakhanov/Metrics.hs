module Database.PostgreSQL.Stakhanov.Metrics
 (
   metrics
 , allMetrics

 ) where

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

