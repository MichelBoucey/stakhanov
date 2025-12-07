module Database.PostgreSQL.Bijou.Types where

import           Data.Aeson.Types
import           Data.Text        as T
import           Numeric.Natural

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md

-- data Message = Message { unMessage :: !Object }
-- newtype Queue = Queue { name :: T.Text }

data Msg = Msg { msgId :: Natural, readCt :: Natural, enqueuedAt :: T.Text, vt :: Natural, message :: !Object }

