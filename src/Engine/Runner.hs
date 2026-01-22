module Engine.Runner
  ( EngineSpec(..)
  , EngineHandle(..)
  , Feeder
  , startEngine
  , runEngineWithQueue
  ) where

import Control.Concurrent.Async (Async, async, link, waitAnyCancel)
import Control.Concurrent.STM
import Control.Monad (forever, void)

-- A feeder is a thread that produces messages and pushes them into the queue.
type Feeder msg = TBQueue msg -> IO ()

data EngineSpec msg = EngineSpec
  { queueDepthES  :: Int
  , workerCountES :: Int
  , feedersES     :: [Feeder msg]
  , workerES      :: Int -> msg -> IO ()
  }

data EngineHandle msg = EngineHandle
  { asyncEH   :: Async ()
  , enqueueEH :: msg -> IO ()
  }

startEngine :: EngineSpec msg -> IO (EngineHandle msg)
startEngine spec = do
  q <- newTBQueueIO (fromIntegral (queueDepthES spec))
  a <- async (runEngineWithQueue spec q)
  pure EngineHandle
    { asyncEH = a
    , enqueueEH = \msg -> atomically (writeTBQueue q msg)
    }

runEngineWithQueue :: EngineSpec msg -> TBQueue msg -> IO ()
runEngineWithQueue spec q = do
  let workerLoop i = forever $ do
        msg <- atomically (readTBQueue q)
        workerES spec i msg

  feederAs <- mapM (\f -> async (f q)) (feedersES spec)
  workerAs <- mapM (\i -> async (workerLoop i)) [1 .. workerCountES spec]

  mapM_ link feederAs
  mapM_ link workerAs

  void $ waitAnyCancel (feederAs ++ workerAs)
