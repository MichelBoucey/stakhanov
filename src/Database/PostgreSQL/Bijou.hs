module Database.PostgreSQL.Bijou where

import           Data.Aeson.Types
import           Data.Int
import           Data.Text                            as T
import           Database.PostgreSQL.Bijou.Statements
import qualified Hasql.Connection                     as C
import qualified Hasql.Session                        as S
import Database.PostgreSQL.Bijou.Types
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

create :: C.Connection -> T.Text -> IO (Either S.SessionError ())
create c q = S.run (S.statement q createQueue) c

drop :: C.Connection -> T.Text -> IO (Either S.SessionError Bool)
drop c q = S.run (S.statement q dropQueue) c

-- Sending Messages

send :: C.Connection -> T.Text -> Value -> IO (Either S.SessionError Int64)
send c q o = S.run (S.statement (q,o) sendMessage) c

sendBatch :: a
sendBatch = undefined

-- Reading Messages

read :: a
read = undefined

readWithPoll :: a
readWithPoll = undefined

pop :: a
pop = undefined

-- Deleting/Archiving Messages

delete :: a
delete = undefined

deleteBatch ::a
deleteBatch =undefined

purgeQueue ::a
purgeQueue = undefined

