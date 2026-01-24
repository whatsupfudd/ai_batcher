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
    , enqueueEH = atomically . writeTBQueue q
    }

runEngineWithQueue :: EngineSpec msg -> TBQueue msg -> IO ()
runEngineWithQueue spec q = do
  let
    workerLoop i = forever $ do
        msg <- atomically (readTBQueue q)
        spec.workerES i msg

  feederAs <- mapM (async . (\f -> f q)) spec.feedersES
  workerAs <- mapM (async . workerLoop) [1 .. spec.workerCountES]

  mapM_ link feederAs
  mapM_ link workerAs

  void . waitAnyCancel $ feederAs <> workerAs
