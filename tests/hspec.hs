{-# LANGUAGE OverloadedStrings #-}

import           Data.Aeson
import qualified Data.Aeson.KeyMap                        as K
import           Data.Int
import qualified Data.Vector                              as V
import qualified Database.PostgreSQL.Stakhanov            as S
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Metrics
import           Database.PostgreSQL.Stakhanov.Types
import           Test.Hspec
import Control.Monad

main :: IO ()
main = hspec $ do

  describe "Create a Hspec test queue" $
      it "Return the record of the created queue" $ do
        Right c <- acquireLocalPGConn
        Right q <- S.create "HspecTestQueue" c
        q `shouldBe` (q :: Queue)

  describe "Send a message" $
      it "Return the ID of message created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            j = Object (K.fromList [("Item", String "Banana"),("Qty", Number 3)])
        S.send q j `shouldReturn` Right 1

  describe "Send a message with options (send')" $
      it "Return the ID of message created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            j1 = Object (K.fromList [("Item", String "Apple"),("Qty", Number 6)])
            j2 = Object (K.fromList [("Metadata", Object (K.fromList [("Checksum", Bool True), ("Lenght", Number 37)]))])
        S.send' q j1 (Just j2) Nothing `shouldReturn` Right 2

  describe "Send a batch of messages" $
      it "Return the IDs of messages created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            o1 = Object (K.fromList [("Item", String "Pineapple"),("Qty", Number 4)])
            o2 = Object (K.fromList [("Item", String "Tomato"),("Qty", Number 9)])
            o3 = Object (K.fromList [("Item", String "strawnberry"),("Qty", Number 23)])
            v  = V.fromList[o1,o2,o3]
        S.batchSend q v `shouldReturn` Right (V.fromList[3,4,5])

  describe "Send a batch of messages with a delay (batchSend')" $
      it "Return the IDs of messages created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            o1 = Object (K.fromList [("Item", String "Potato"),("Qty", Number 16)])
            o2 = Object (K.fromList [("Item", String "Carrot"),("Qty", Number 21)])
            o3 = Object (K.fromList [("Item", String "strawnberry"),("Qty", Number 23)])
            v  = V.fromList[o1,o2,o3]
        S.batchSend' q v Nothing (Just (InSeconds 1)) `shouldReturn` Right (V.fromList[6,7,8])

  describe "Send a batch of messages with headers (batchSend')" $
      it "Return the IDs of messages created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            o1 = Object (K.fromList [("Item", String "Potato"),("Qty", Number 8)])
            o2 = Object (K.fromList [("Item", String "Carrot"),("Qty", Number 120)])
            o3 = Object (K.fromList [("Item", String "strawnberry"),("Qty", Number 1230)])
            o4 = Object (K.fromList [("Metadata", Object (K.fromList [("Checksum", Bool True), ("Lenght", Number 37)]))])
            o5 = Object (K.fromList [("Metadata", Object (K.fromList [("Checksum", Bool True), ("Lenght", Number 40)]))])
            o6 = Object (K.fromList [("Metadata", Object (K.fromList [("Checksum", Bool True), ("Lenght", Number 500)]))])
            v1 = V.fromList[o1,o2,o3]
            v2 = V.fromList[o4,o5,o6]
        S.batchSend' q v1 (Just v2) Nothing `shouldReturn` Right (V.fromList[9,10,11])

  describe "Get the current length of the queue, extracted from its metrics" $
      it "Return the number of messages currently in the queue" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right q' <- metrics q
        (pure $ getQueueLength q') `shouldReturn` Just 11

  describe "Get metrics of all queues" $
      it "Return a vector of queues with their metrics embbeded" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right vq <- allMetrics q
        vq `shouldBe` (vq :: V.Vector Queue)

  describe "Read a message" $
      it "Maybe returns a vector of messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right vm <- S.read q 30 1
        vm  `shouldBe` (vm :: Maybe Messages)

  describe "Archive a message" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.archive q 1 `shouldReturn` Right True

  describe "Archive messages" $
      it "Return the IDs of archived messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            v = V.fromList[3,4,5]
        S.batchArchive q v `shouldReturn` Right v

  describe "Delete a message" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.delete q 2 `shouldReturn` Right True

  describe "Set Visibility Timeout of the given message IDs (batchSetVT)" $
      it "Return the vector of updated messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            v = V.fromList[6,7,8]
        Right vm <- S.batchSetVT q v 30
        vm `shouldBe` (vm :: V.Vector Message)

  describe "Delete messages" $
      it "Return the vector of IDs of deleted messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            vi = V.fromList[6,7,8]
        S.batchDelete q vi `shouldReturn` Right vi

  describe "Pop messages from the queue" $
      it "Maybe return messages and delete them from the queue" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right p <- S.pop q 2
        p `shouldBe` (p :: Maybe Messages)

  describe "Purge the Hspec test queue" $
      it "Return the number of deleted messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right n <- S.purge q
        n `shouldBe` (n :: Int64)

  describe "Get the list of queues with just a connection" $
      it "Return a vector of queues)" $ do
        Right c <- acquireLocalPGConn
        Right v <- S.listQueues c
        v `shouldBe` (v :: V.Vector Queue)

  describe "Get the list of queues with the connection embbeded in a queue" $
      it "Return a vector of queues" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right v <- S.listQueues' q
        v `shouldBe` (v :: V.Vector Queue)

  describe "Get a detail of the queue details" $
      it "Return Just False" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right v <- S.listQueues' q
        let Just q' = S.details q v
        (pure $ S.getIsUnlogged q') `shouldReturn` Just False

  describe "Delete the queue" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.drop q `shouldReturn` Right True

