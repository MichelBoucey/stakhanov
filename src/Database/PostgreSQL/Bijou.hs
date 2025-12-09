module Database.PostgreSQL.Bijou
 ( conn --module Database.PostgreSQL.Bijou.Connection
-- , create
-- , drop
-- , read
 )

where

import           Data.Aeson.Types
import           Data.Int
import           Data.Text                            as T
import           Data.Time
import           Data.Vector                          hiding (create)
import           Database.PostgreSQL.Bijou.Connection
import           Database.PostgreSQL.Bijou.Statements
import           Database.PostgreSQL.Bijou.Types
import qualified Hasql.Connection                     as C
import qualified Hasql.Session                        as S

-- https://hackage.haskell.org/package/hasql
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

create :: C.Connection -> T.Text -> IO (Either S.SessionError ())
create c q = S.run (S.statement q createQueue) c

drop :: C.Connection -> T.Text -> IO (Either S.SessionError Bool)
drop c q = S.run (S.statement q dropQueue) c

-- Sending Messages

send :: C.Connection -> T.Text -> Value -> IO (Either S.SessionError Int64)
send c q v = S.run (S.statement (q,v) sendMessage) c

sendBatch :: a
sendBatch = undefined

-- Reading Messages

-- Should be : read :: C.Connection -> T.Text -> Int32 -> Int32 -> IO (Either S.SessionError (Maybe Messages))
read :: C.Connection -> T.Text -> Int32 -> Int32 -> IO (Either S.SessionError (Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value)))
read c q v q' = S.run (S.statement (q,v,q') readMessages) c

-- TODO : readWithPoll

pop :: a
pop = undefined

-- Deleting/Archiving Messages

delete :: a
delete = undefined

deleteBatch ::a
deleteBatch =undefined

purgeQueue ::a
purgeQueue = undefined

