import sys
import os

ackFIFO = None
dataFIFO = None

ackString = "ack"
nackString = "nck"

#TODO: camelCase for function and method names ?
#TODO: opt out for exception hook

def gatkExceptionHook(exceptionType, value, traceback):
    sendNack()
    sys.__excepthook__(exceptionType, value, traceback)

def onTraversalStart(ackFIFOName: str):
    global ackFIFO
    # the exception hook uses the ack FIFO, so establish the FIFO first
    ackFIFO = AckFIFO(ackFIFOName)
    sys.excepthook = gatkExceptionHook

def onTraversalSuccess():
    pass

def closeTool():
    global ackFIFO
    assert ackFIFO != None
    ackFIFO.close()

def sendAck():
    global ackFIFO
    ackFIFO.writeAck()

def sendNack():
    global ackFIFO
    ackFIFO.writeNack()

def initializeDataFIFO(dataFIFOName: str):
    global dataFIFO
    dataFIFO = DataFIFO(dataFIFOName)

def closeDataFIFO():
    global dataFIFO
    assert dataFIFO != None
    dataFIFO.close()

def readDataFIFO() -> str:
    global dataFIFO
    return dataFIFO.readLine()

class AckFIFO:

    def __init__(self, ackFIFOName: str):
        '''open the input ack fifo stream for writing only
        '''
        self.ackFIFOName = ackFIFOName
        writeDescriptor = os.open(self.ackFIFOName, os.O_WRONLY)
        self.fileWriter = os.fdopen(writeDescriptor, 'w')

    def writeAck(self):
        assert self.fileWriter != None
        self.fileWriter.write(ackString)
        self.fileWriter.flush()

    def writeNack(self):
        assert self.fileWriter != None
        self.fileWriter.write(nackString)
        self.fileWriter.flush()

    def close(self):
        assert self.fileWriter != None
        self.fileWriter.close()
        self.fileWriter = None

class DataFIFO:

    #TODO: Need a way to provide a per-line deserializer

    def __init__(self, dataFIFOName: str):
        '''open the input data stream fifo for reading
        '''
        self.dataFIFOName = dataFIFOName

        #the data fifo is always opened for read only on the python side
        readDescriptor = os.open(self.dataFIFOName, os.O_RDONLY)
        self.fileReader = os.fdopen(readDescriptor, 'r')

    def readLine(self) -> str:
        assert self.fileReader != None
        return self.fileReader.readline()

    def close(self):
        assert self.fileReader != None
        self.fileReader.close()
        self.fileReader = None