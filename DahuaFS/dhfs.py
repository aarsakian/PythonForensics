
from struct import unpack, pack
from dataclasses import dataclass
import datetime
import re
import sys
import os
import time
import asyncio
import logging
import ffmpeg
import threading
import concurrent
from threading import Thread
from queue import Queue

#import pyewf


logfilename = "dhav"+str(datetime.datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


#print (pyewf.get_version())

NTHREADS = 10

FRAME_START_ID = b'\x44\x48\x41\x56' # DHAV
FRAME_END_ID = 0x6468676
FRAME_SIZE = 2**30
TAIL_FRAME_SIZE = 8
FRAME_HEADER_SIZE = 30
FIRST_FRAME = 0xfd
SECONDS_DIFFERENCE = 2
MIN_DURATION = 1 #secs

FRAME_START_ID_REGEX = re.compile(FRAME_START_ID )
QUALITY = 12


BLOCK_SIZE = 2*1000*1000*1024


class Dav2MP4Pipe(Queue):
    def __init__(self, dav_file, output_folder, channel):
   
        self.dav_file = dav_file
        self.mp4_file = dav_file.split(".")[0] + ".mp4"
        self.output_folder =output_folder
        self.channel = channel

 
def consumer_frames(output_folder):
    while True:
      
        try:
            dav_file, channel = queue.get()
            mp4_file = dav_file.split(".")[0] + ".mp4"
            logging.info("converting file {}".format(dav_file))
            print("converting file {}".format(dav_file))

            out, err = ffmpeg.input(os.path.join(output_folder, str(channel), dav_file))\
            .output(os.path.join(output_folder, str(channel), mp4_file))\
            .run(capture_stdout=True, capture_stderr=True)
           
            logging.info("out {}".format(out))
            logging.info("err {}".format(err))
            queue.task_done()
        except ffmpeg.Error as e:
            print(e)
       


def convert_raw_time_to_dhfs_format(raw_data):
    year = raw_data >> 26
    month = (raw_data & 62914560) >> 22
    day = (raw_data & 4063232) >> 17
    hour = (raw_data & 126976) >> 12
    minutes = (raw_data & 4032) >> 6
    seconds = (raw_data & 63)
    return year, month, day, hour, minutes, seconds



class Frames:
    def __init__(self, head, tail=None, duration=0):
        self.head = head
        self.tail = tail
        self.duration = duration

    def write(self, path):
        frame = self.head
        tail_frame = self.tail 
        
        self.fname = to_str(frame.date()) + "_" + to_str(tail_frame.date()) + "____" + str(frame.start) + "----" + str(tail_frame.end) + ".dav"
        base_path = frame.header.get_path_using_channel(path)
        
        self.channel = frame.header.channel

        content = frame.content
        while(frame.next):
            frame = frame.next
            content += frame.content
      
        if not os.path.exists(base_path):
            os.mkdir(base_path)
        
        with open(os.path.join(base_path, self.fname), 'wb') as f:
            f.write(content)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
            print("{} created frame {} length {}".format(now, self.fname, len(content)))


def to_str(date_time):
         return date_time.strftime('%Y-%m-%d %H_%M_%S')


@dataclass 
class FrameTime:
    
                          
    """
        holds datetime information
    """

    def __init__(self, year:int,
                month:int,
                day:int,
                hour:int,
                minutes:int,
                seconds:int):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minutes = minutes
        self.seconds = seconds
        self.corrupted = False
        self._to_date()

    def __bytes__(self):
        return self.raw.to_bytes(4, byteorder='little')

    def _to_date(self):
        try:
            self.f_date = datetime.datetime(2000+self.year, self.month,
                                          self.day, self.hour,
                                          self.minutes, self.seconds)

        except ValueError as e:
            logging.error("error parsing date   {}".format(e))
            self.corrupted = True
            
            


@dataclass 
class FrameHeader:
    """
        holds header information 
    """
    def __init__(self, raw):
        self.raw = raw
        self.corrupted = False
        unpack('<4sHHLL', raw[:16])
        self.start_identifier, self.type, self.channel_, self.number, self.length = \
            unpack('<4sHHLL', raw[:16])

        frame_time_raw = int.from_bytes(raw[16:20], byteorder='little')
        self.quality = int.from_bytes(raw[29:30], byteorder='big')
        self.time_frame = FrameTime(*convert_raw_time_to_dhfs_format(frame_time_raw))

    def __bytes__(self):
        return pack('<4sHHLLH', *self[:-1]) + \
                    bytes(self.time_frame)

    @property
    def channel(self):
        return self.channel_ + 1

    def create_filename(self):
        return to_str(self.time_frame.f_date) + "_seq" + str(self.number) + ".dav"

    def get_path_using_channel(self, path):
        return os.path.join(path, str(self.channel))

    def is_corrupted(self):
        return self.time_frame.corrupted


@dataclass
class FrameTail:
    """
        holds tail information 
 
    """
    def __init__(self, raw:bytes, corrupted:bool=False):
        if len(raw) == TAIL_FRAME_SIZE:
            self.end_identifier, self.tail = unpack('<4sL', raw)
        else:
            self.corrupted = True

    def __bytes__(self):
        return pack('<4sL', *self)


@dataclass
class Frame:
 

    """
        holds frame information
    """

    def __init__(self, content:bytes, header:FrameHeader,
                tail:FrameTail):
        self.content = content
        self.header = header
        self.tail = tail
        self.prev = None
        self.next = None

    def set_offsets(self, start, end):
        self.start = start
        self.end = end

    def __bytes__(self):
        return bytes(self.content)
    
    def date(self):
        return self.header.time_frame.f_date

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
            now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
            print("{} created frame {} length {}".format(now, filename, len(self.content)))

    @property
    def length(self):
        return self.header.length

    def is_corrupted(self):
        return self.header.is_corrupted() or self.tail.corrupted
    

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
        round = 0
        while True:
            next(self.read_co)
            raw_data = self.read_co.send((self.file_offset, BLOCK_SIZE))
            logging.info("block from {} to {} ".format(self.file_offset, self.file_offset+len(raw_data)))

            if not raw_data:
                break
            previous_frame = None
         
            for match in FRAME_START_ID_REGEX.finditer(raw_data):
               
                self.file_offset = match.start()
                
             
                header = FrameHeader(raw_data[self.file_offset:self.file_offset+FRAME_HEADER_SIZE])      

                if header.is_corrupted():
                    logging.warning("frame at {} is corrupted".format(self.file_offset))
                    print("frame skipped offset {}".format(self.file_offset))
                    continue
       
                tail = FrameTail(raw_data[self.file_offset+header.length- 8:self.file_offset+header.length])
     
                frame = Frame(raw_data[self.file_offset:self.file_offset+header.length], header, tail)
                
                frame.set_offsets(self.file_offset, self.file_offset+header.length)

                if not previous_frame and frame.is_first():
                    file_offset_beg = self.file_offset
                    previous_frame = frame
                  
                    total_duration = 0 
                    frames = Frames(frame)
                    logging.info("first frame found at {}".format(self.file_offset))
                    continue
              
                if previous_frame:
                    
                    time_diff =  frame.date() - previous_frame.date()
                  
                  #  logger.info("checking for frame at offset {} sequence {} type {} channel {} time diff {} date {}".format(
                 #       self.file_offset, frame.header.number,frame.header.type,  frame.header.channel, time_diff, frame.date()))
                 
                    if time_diff.seconds < SECONDS_DIFFERENCE and previous_frame.header.channel == frame.header.channel:
     
                        frame.prev = previous_frame
                        previous_frame.next = frame

                        total_duration += time_diff.seconds
                        #frame = Frame(previous_frame.content + frame.content, frame.header, frame.tail)
                    else:
                        file_offset_end = self.file_offset - 1
                     #   logger.info("export channel {} offset start {} end {} duration {}".format(previous_frame.header.channel,
                     #                                                                      file_offset_beg, file_offset_end, total_duration))
                        if total_duration > MIN_DURATION:
                            frames.tail = previous_frame
                            frames.duration = total_duration
                            yield frames
                         
                        else:
                            logging.info("frame NOT exported REASON:"
                                         "small duration channel {} offset start {}"
                                         "end {} duration {}".format(previous_frame.header.channel,
                                                                                           file_offset_beg, file_offset_end, total_duration))
                      
                        frames = Frames(frame)
                        total_duration = 0
                        file_offset_beg = self.file_offset 

                   
                    previous_frame = frame
            
            
            self.file_offset += round*BLOCK_SIZE  
            round += 1
               #     logger.info("Found frame at offset {} length {} ".format(self.file_offset, frame.length))    
     
           
                

def producer_frames(data, output_folder, start_offset, queue):
    parser = Parser(data, start_offset)

    nof_frames = 0

    for frames in parser.find_frames():
      frames.write(output_folder)
      queue.put((frames.fname, frames.channel))
          
      nof_frames += 1
    return nof_frames
        
        
      

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

    fsize = os.stat(sys.argv[1]).st_size - start_offset
    if fsize < BLOCK_SIZE:
        BLOCK_SIZE = fsize

    queue = Queue()

    for i in range(NTHREADS):
       thread = Thread(target=consumer_frames, args=(sys.argv[2],))
       thread.setDaemon(True)
       thread.start()

    nof_frames = producer_frames(sys.argv[1], sys.argv[2], start_offset, queue)


    queue.join()
    

   
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

    duration = t2 - t1
    rate = fsize/(1024*1000*duration)
    logging.info(" processed {} bytes in {} rate {} MB/sec from which {} frames extracted ".format(str(fsize), str(duration), str(rate), str(nof_frames)))
    print(" processed {} bytes in {} rate {} MB/sec from which {} frames extracted ".format(str(fsize), str(duration), str(rate), str(nof_frames)))
