#!/usr/bin/python 

import threading
import time
from Queue import Queue, Empty

class MainLoopError(Exception):
    #used internally
    def __init__(self, message, okToContinue = True):
        super(MainLoopError, self).__init__(message)
        self.okToContinue = okToContinue

class TaskError(Exception):
    #raised by doTask when a error is encountered executing tasks
    #set okToContinue to False will terminate the thread
    def __init__(self, message, okToContinue = True):
        super(TaskError, self).__init__(message)
        self.okToContinue = okToContinue
        
class PrepError(Exception):
    #raised by prepare when an error is encountered during preparation
    #a PrepError will cause the thread to terminate immediately
    pass
        
class BaseCharacter(threading.Thread):
    def __init__(self):
        super(BaseCharacter, self).__init__()
        #both queues are vitrual member to be filled at runtime
        self._srcq = None
        self._destq = None
        self.goodToGoLock = threading.Lock()
        self.goodToGo = True
        #args are to be filled in at runtime
        self._mainLoopArgs = {}
        self._prepArgs = {}
        self._finishArgs = {}
        self._prepFailArgs = {}
        self._cleanUpArgs = {}
        self._pausedEvent = threading.Event()
        self._pausedEvent.set()
        
    def getGoodToGoLock(self):
        return self.goodToGoLock
        
    def setGoodToGo(self, val):
        self.goodToGoLock.acquire()
        self.goodToGo = val
        self.goodToGoLock.release()
        return val
        
    def _isGoodToGo(self):
        self.goodToGoLock.acquire()
        isGoodToGo = self.goodToGo
        self.goodToGoLock.release()
        return isGoodToGo
        
    def pause(self):
        self._pausedEvent.clear()
        
    def resume(self):
        self._pausedEvent.set()
        
    def isPaused(self):
        return not self._pausedEvent.isSet() #pausedevent: true, good to go; false paused
        
    def run(self):
        try:
            self.prepare(**self._prepArgs)
        except PrepError as e:
            self.onPrepFailed(exception = e, **self._prepFailArgs)
        else:
            while self._isGoodToGo():
                self._pausedEvent.wait()
                try:
                    self.mainLoop(**self._mainLoopArgs)
                except MainLoopError as e:  
                    if e.okToContinue:
                        print "Thread [%s] encountered minor error(%s)" % (self.name, e)
                        continue
                    else:
                        self.cleanUp(exception = e, **self._cleanUpArgs)
                        return
            self.finish(**self._finishArgs)
            
    def mainLoop(self, *args, **kwargs):
        #virtual method to be overrided
        raise MainLoopError("mainLoop not implemented", False)
    
    def prepare(self, *args, **kwargs):
        #virtual method to be overrided
        #raise PrepError when preparation failed
        print "Thread [%s] finished preparation succeefully!" % self.name
        
    def finish(self, *args, **kwargs):
        #vitual method to be overrided
        print "Thread [%s] finished succeefully!" % self.name

    
    def cleanUp(self, exception, *args, **kwargs):
        #vitual method to be overrided
        print "Thread [%s] exited with error(%s)" % (self.name, exception)

    
    def onPrepFailed(self, exception, *args, **kwargs):
        #virtual method to be overrided
        print "Thread [%s] preparation failed!(%s) " % (self.name, exception)

            

class Producer(BaseCharacter):
        
    def __init__(self, destQueue, taskArgs = {}):
        super(Producer, self).__init__()
        self._destq = destQueue
        self._taskArgs = taskArgs
        
    def mainLoop(self):
        try:
            raw = self.doTask(**self._taskArgs)
        except TaskError as e:
            if e.okToContinue:
                raise MainLoopError(("task minor error: %s" % e), True)
            else:
                raise MainLoopError(("task fatal error: %s" % e), False)
        else:
            self._destq.put(raw)
        
    def doTask(self, *args, **kwargs):    
        #virtual method to be overrided
        return None

            
class Consumer(BaseCharacter):
    def __init__(self, srcQueue, taskArgs = {}):
        super(Consumer, self).__init__()
        self._GET_QUEUE_TIMEOUT = 1
        self._srcq = srcQueue
        self._taskArgs = taskArgs
        
    def mainLoop(self):
        try:
            goods = self._srcq.get(block = True, timeout = self._GET_QUEUE_TIMEOUT)
        except Empty as e:
            return #nothing to process
        else:
            self._srcq.task_done()
            
        try:
            self.doTask(goods = goods, **self._taskArgs)
        except TaskError as e:
            if e.okToContinue:
                raise MainLoopError(("task minor error: %s" % e), True)
            else:
                raise MainLoopError(("task fatal error: %s" % e), False)
        
    def doTask(self, goods, *args, **kwargs):
        #virtual method to be overrided
        return None
        
class Manufacturer(BaseCharacter):
    def __init__(self, srcQueue, destQueue, taskArgs = {}):
        super(Manufacturer, self).__init__()
        self._GET_QUEUE_TIMEOUT = 1
        self._srcq = srcQueue
        self._destq = destQueue
        self._taskArgs = taskArgs

    def mainLoop(self):
        try:
            raw = self._srcq.get(block = True, timeout = self._GET_QUEUE_TIMEOUT)
        except Empty as e:
            return #nothing to process
        else:
            self._srcq.task_done()
        
        try:
            product = self.doTask(raw = raw, **self._taskArgs)
        except TaskError as e:
            if e.okToContinue:
                raise MainLoopError(("task minor error: %s" % e), True)
            else:
                raise MainLoopError(("task fatal error: %s" % e), False)
        else:
            self._destq.put(product)
        
    def doTask(self, raw, *args, **kwargs):
        #virtual method to be overrided
        return None
          