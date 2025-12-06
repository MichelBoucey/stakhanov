module Database.PostgreSQL.Bijou.Connection where

import           Hasql.Connection
import           Hasql.Connection.Setting            (connection)
import           Hasql.Connection.Setting.Connection (string)

conn :: IO (Either ConnectionError Connection)
conn = acquire [connection $ string "postgres://postgres:postgres@0.0.0.0:5432/postgres"]

