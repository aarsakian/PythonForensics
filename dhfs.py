

from struct import unpack, pack
from collections import namedtuple
import datetime
import re
import sys
import os
import time
import asyncio
import logging
#import pyewf


logfilename = "dhav"+str(datetime.datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


#print (pyewf.get_version())

FRAME_START_ID = b'\x44\x48\x41\x56' # DHAV
FRAME_END_ID = 0x6468676
FRAME_SIZE = 2**30
FRAME_HEADER_SIZE = 30
FIRST_FRAME = 0xfd
SECONDS_DIFFERENCE = 2

FRAME_START_ID_REGEX = re.compile(FRAME_START_ID )
QUALITY = 12


BLOCK_SIZE = 2*1000*1000*1024


class FrameNode:
    def __init__(self, frame):
        self.frame = frame
        self.next = None
        self.prev = None



class Frames:
    def __init__(self, head):
        self.head = None


def to_str(date_time):
         return date_time.strftime('%Y-%m-%d %H_%M_%S')


class FrameTime(namedtuple('FrameTime',
                           'year month day hour minutes seconds raw')):
    """
        holds datetime information use slots for memory efficiency
        restricts attributes
    """

    __slots__ = ()

    def __bytes__(self):
        return self.raw.to_bytes(4, byteorder='little')

    def to_date(self):
        try:
            date_time = datetime.datetime(2000+self.year, self.month,
                                          self.day, self.hour,
                                          self.minutes, self.seconds)

        except ValueError as e:
            logging.error("error parsing date continuing {}".format(e))
            print("error parsing date continuing")
            date_str = "corrupted_date_seq.dav"
        finally:
            return date_time


class FrameHeader(namedtuple('FrameHead', 'start_identifier type channel_ number\
                       length quality time_frame')):
    """
        holds header information use slots for memory efficiency
        restricts attributes
    """

    __slots__ = ()

    def __bytes__(self):
        return pack('<4sHHLLH', *self[:-1]) + \
                    bytes(self.time_frame)

    @property
    def channel(self):
        return self.channel_ + 1

    def create_filename(self):
        date_str = to_str(self.time_frame.to_date())
        return date_str + "_seq" + str(self.number) + ".dav"

    def get_path_using_channel(self, path):
        return os.path.join(path, str(self.channel))


class FrameTail(namedtuple('FrameTail', 'end_identifier length')):
    """
        holds tail information use slots for memory efficiency
        restricts attributes
    """
    __slots__ = ()

    def __bytes__(self):
        return pack('<4sL', *self)


class Frame(namedtuple('Frame', 'header content tail')):
    """
        holds frame information use slots for memory efficiency
        restricts attributes
    """
    __slots__ = ()

    def __bytes__(self):
        return bytes(self.content)
    
    def date(self):
        return self.header.time_frame.to_date()

    def is_first(self):
        return self.header.type == FIRST_FRAME

    def create_filename(self):
        return self.header.create_filename()

    def get_offsets(self):
        return (header.beg_offset, header.beg_offset+self.length())

    def write(self, path):
        filename = self.create_filename()
        base_path = self.header.get_path_using_channel(path)
        if not os.path.exists(base_path):
            os.mkdir(base_path)
        with open(os.path.join(base_path, filename), 'wb') as f:
            f.write(bytes(self))
            print("created frame {} length {}".format(filename, len(self.content)))

    @property
    def length(self):
        return self.header.length
    

class Parser:

    def __init__(self, filename, start_offset):
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        self.read_co = self._read_data_co(filename)
        self.file_size = os.stat(filename).st_size
        self.file_offset = int(start_offset)
        self.step = 1

    def _read_data_co(self, filename):
        """
        a generator based coroutine that reads from the file
        to be parsed. It keeps the file handle while extracting data.
        :param filename: the name of the filename to be read
        :return:
        """
        with open(filename, 'rb') as f:

            while True:
                offset, data_length = yield
                f.seek(offset)
                yield f.read(data_length)

    def find_frames(self):
        while True:
            next(self.read_co)
            raw_data = self.read_co.send((self.file_offset, self.file_offset+BLOCK_SIZE))
            print(self.file_offset, len(raw_data))
            if not raw_data:
                break
            previous_frame = None
         
            for match in FRAME_START_ID_REGEX.finditer(raw_data):
                self.file_offset = match.start()
                
                frame = _create_frame(raw_data[self.file_offset:self.file_offset+FRAME_SIZE]) # guessing
                
                if not previous_frame and frame.is_first():
                    file_offset_beg = self.file_offset
                    previous_frame = frame
                    prev_frame_node = FrameNode(frame)
                    total_duration = 0 
                    frames = Frames(prev_frame_node)
                    logging.info("first frame found at {}".format(self.file_offset))
                    continue
               #frame = _create_frame_fast(raw_data[self.file_offset:self.file_offset+FRAME_SIZE]) # guessing
              
                if previous_frame:
                 
                    time_diff =  frame.date() - previous_frame.date()
                    logger.info("checking for frame at offset {} sequence {} type {} channel {} time diff {} date {}".format(
                        self.file_offset, frame.header.number,frame.header.type,  frame.header.channel, time_diff, frame.date()))

                    if time_diff.seconds < SECONDS_DIFFERENCE and previous_frame.header.channel == frame.header.channel:
                        
                        frame = _concatenate_frames(previous_frame, frame)
                        
                        frame_node = FrameNode(frame)
                        frame_node.prev = prev_frame_node
                        prev_frame_node.next = frame_node 

                        total_duration += time_diff.seconds
                    else:
                        file_offset_end = self.file_offset - 1
                        logger.info("export frame channel {} offset start {} end {} duration".format(previous_frame.header.channel,
                                                                                            file_offset_beg, file_offset_end, total_duration))
                        yield previous_frame  

                        prev_frame_node = FrameNode(frame)
                        frames = Frames(prev_frame_node)
                        
                        total_duration = 0
                        file_offset_beg = self.file_offset 

                    previous_frame = frame
               # if frame.length < FRAME_SIZE and frame.header.quality == QUALITY:  # must be valid header    
               #     logger.info("Found frame at offset {} length {} ".format(self.file_offset, frame.length))    
     
            self.file_offset += BLOCK_SIZE
                

def _concatenate_frames(previous_frame, frame):
    return Frame._make([frame.header, previous_frame.content + frame.content, frame.tail])


def _create_frame(raw_data):
       
    header = _create_frame_head(raw_data[:FRAME_HEADER_SIZE])
    raw_data = raw_data[:header.length] # reset ending
    tail = _create_frame_tail(raw_data[-8:])
    return Frame._make([header, raw_data, tail])


def _create_frame_fast(raw_data):
    start_identifier, type, channel, number, length = \
        unpack('<4sHHLL', raw_data[:16])
    frame_time_data = int.from_bytes(raw_data[16:20], byteorder='little')
    quality = int.from_bytes(raw_data[29:30], byteorder='big')
    year = frame_time_data >> 26
    month = (frame_time_data & 62914560) >> 22
    day = (frame_time_data & 4063232) >> 17
    hour = (frame_time_data & 126976) >> 12
    minutes = (frame_time_data & 4032) >> 6
    seconds = (frame_time_data & 63)
    date_time = datetime.datetime(2000+year, month,
                                          day, hour,
                                          minutes, seconds)
    date_str = date_time.strftime('%Y-%m-%d %H_%M_%S')
    return channel, number, date_str, raw_data[:length]
    

def _create_frame_time(raw_data):
  
    year = raw_data >> 26
    month = (raw_data & 62914560) >> 22
    day = (raw_data & 4063232) >> 17
    hour = (raw_data & 126976) >> 12
    minutes = (raw_data & 4032) >> 6
    seconds = (raw_data & 63)
  
    return FrameTime._make(
        [year, month, day, hour, minutes, seconds, raw_data])


def _create_frame_head(raw_data):
    start_identifier, type, channel, number, length = \
        unpack('<4sHHLL', raw_data[:16])
   
    frame_time_data = int.from_bytes(raw_data[16:20], byteorder='little')
    quality = int.from_bytes(raw_data[29:30], byteorder='big')
    frame_time = _create_frame_time(frame_time_data)
   
    return FrameHeader._make(
        [start_identifier, type, channel, number, length, quality, frame_time ])


def _create_frame_tail(raw_data):
    return FrameTail._make(unpack('<4sL', raw_data))





def produce_n_consume_frames(data, output_folder, start_offset):
    parser = Parser(data, start_offset)
    [frame.write(output_folder) for frame in parser.find_frames()]
      

def produce_n_consume_frames_fast(data, output_folder, start_offset):
    parser = Parser(data, start_offset)
    for channel, number, date_str, frame in parser.find_frames():
        filename = date_str + "_seq" + str(number) + ".dav"
        base_path = os.path.join(output_folder, str(channel))
        with open(os.path.join(base_path, filename), 'wb') as f:
            f.write(frame)
            print("wrote frame",filename)
          

@asyncio.coroutine
def produce_frames(data, start_offset, queue):
    parser = Parser(data, start_offset)
    for frame in parser.find_frames(): # not asynchronous operation
        queue.put_nowait(frame)
    queue.put_nowait(None)


@asyncio.coroutine
def consume_frames(work_queue, output_folder):
    while True:
        frame = yield from work_queue.get()
        if frame is None:
            break
        frame.write(output_folder)

if __name__ == "__main__":
#def main():

    if len(sys.argv) < 3:
        sys.stdout.write("Please provide file to be parsed and export folder location!\n")
        sys.exit()

    t1 = time.time()
    
    if len(sys.argv) == 4:
        start_offset = sys.argv[3]
    else:
        start_offset = 0

    #produce_n_consume_frames_fast(sys.argv[1], sys.argv[2], start_offset)
    produce_n_consume_frames(sys.argv[1], sys.argv[2], start_offset)
        
    t2 = time.time()

    t3 = time.time()
    if len(sys.argv) == 5:
        queue = asyncio.Queue()
        loop = asyncio.get_event_loop()
        producer = consumer = asyncio.ensure_future(produce_frames(sys.argv[1], start_offset, queue))
        consumer = asyncio.ensure_future(consume_frames(queue,sys.argv[2]))

        loop.run_until_complete(asyncio.gather(producer, consumer))
        loop.close()

    t4 = time.time()
    print("Extraction process completed!")
    print(" took " + str(t2 - t1) + " seconds vs for asyncio" +str(t4-t3) )
