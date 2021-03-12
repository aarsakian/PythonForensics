# fVfE
import os, datetime, struct, sys, binascii, time, subprocess
from struct import unpack 
import logging
import ffmpeg
import threading
from queue import Queue
from threading import Thread
from collections import defaultdict

logfilename = "avr"+str(datetime.datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


OUT_PATH = './out/'
SECONDS_DIFFERENCE = 2
NTHREADS = 10

MAX_FILE_HANDLES = 490


def create_srt_file(mp4_file):
    date_start_, date_end_ = mp4_file.split(".mp4")[0].split(" ")
    srt_file = mp4_file.replace(".mp4", ".srt")
    try:
        result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', mp4_file], 
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        date_start_ = date_start_.split("\\")[-1]
        date_start = datetime.datetime.strptime(date_start_, '%d_%m_%YT%H_%M_%S')
        date_end = datetime.datetime.strptime(date_end_, '%d_%m_%YT%H_%M_%S')
    
        print('Creating srt for file {}'.format(mp4_file))
        logging.info('Creating srt for file {}'.format(mp4_file))
        duration = int(float(result.stdout))
    except ValueError:
        logging.error('error when creating srt for file {}'.format(mp4_file))
        return 
        
    with open(srt_file, 'w') as f:
    
        for  sec in range(0, duration):
            start = str(datetime.timedelta(seconds=sec))
            end =  str(datetime.timedelta(seconds=sec+1))
            f.write(str(sec+1) + '\n')
            f.write(start + ' --> ' + end + '\n')
            f.write('Estimated Time ' + datetime.datetime.strftime(date_start + datetime.timedelta(seconds=sec), '%d/%m/%Y %H:%M:%S') + '\n\n')
     
            

def analyze_timestamps(channel, all_frames):
    files_chain = []
    
    for channel, dates in all_frames.items():
        dates.sort(key=lambda d:d[0])  # sort by date

        prev_date_start, prev_date_end = dates[0]
      
        beginning_time = prev_date_start
        
        for idx, (date_start, date_end) in enumerate(dates[1:]):
                    time_diff = prev_date_end - date_start
                  
                    if  time_diff.seconds > 1 or time_diff.seconds < -1:#1 
                        
                  
                        last_time = prev_date_end 
                        logging.info("Ch {} Interval from {} to {}".format(channel, beginning_time, last_time))
                        beginning_time = date_start
                    prev_date_start = date_start
                    prev_date_end = date_end   
     
        logging.info("Ch {} Interval from {} to {}".format(channel, beginning_time, date_end))

    
      
        

def log_record_times_produce_srt(dst):
    for idx, (root, _, files) in  enumerate(os.walk(dst)):
        
        if "Cam" in root:
            
            channel = root.split('\\')[1]
       # print(channel)
        all_frames = defaultdict(list)
        for file in files:  
            if file.endswith("mp4"):
                date_start, date_end = file.split(".")[0].split(" ")
                
                all_frames[channel].append((datetime.datetime.strptime(date_start, '%d_%m_%YT%H_%M_%S'),
                                           datetime.datetime.strptime(date_end, '%d_%m_%YT%H_%M_%S')))
    
                create_srt_file(os.path.join(root, file))
               
        if all_frames:       
            analyze_timestamps(channel, all_frames)


def create_backup_txt_file(files_chain, mp4_file):
    txt_file = mp4_file.split(".")[0] + ".txt"
    base_path = os.path.abspath(sys.argv[2])
    with open(txt_file, 'w') as f:
        for file in files_chain:
            file = base_path + file
           
            f.write('file ' + file.replace('\\', '\\\\').replace(' ', '\ ') + '\n')

    

### threaded version
def consumer_frames(output_folder):
    while True:
        try:
            arv_file, channel = queue.get()
            mp4_file = arv_file.split(".")[0] + ".mp4"
            
          
            logging.info("converting file {}".format(arv_file))
            print("converting file {}".format(arv_file))

            out, err = ffmpeg.input(os.path.join(output_folder, str(channel), arv_file))\
            .output(os.path.join(output_folder, str(channel), mp4_file))\
            .run(capture_stdout=True, capture_stderr=True)
      
            #logging.info("out {}".format(out))s
            logging.info("err {}".format(err))
           
        except ffmpeg.Error as e:
            print(e)
        finally:
            queue.task_done()
       


def consume_frames(files_chain):
        start_date = files_chain[0].split(" ")[0]
        end_date = files_chain[-1].split(" ")[-1].split(".")[0]
        mp4_file = os.path.join(start_date + " " + end_date + ".mp4")
        files_chain_joined = "|".join(files_chain)
        
        try:
                         
            if not os.path.exists(mp4_file):
            
                logging.info("converting files {} ".format(mp4_file))
                print("converting files {}".format(mp4_file))
                out, err = ffmpeg.input('concat:{}'.format(files_chain_joined)).\
                output(mp4_file, acodec='copy').run(capture_stdout=True, capture_stderr=True)
                print(err)
            else:
                print("skipped {}".format(mp4_file))
                logging.info("file {} already created".format(mp4_file))
            
           
        except ffmpeg.Error as e:
            print(e.stderr.decode('utf8'))
            logging.error("err {}".format(e.stderr.decode('utf8')))
            create_backup_txt_file(files_chain, mp4_file)
            if len(files_chain) > 1:
                logging.info("try again with less frames len {}".format(len(files_chain)))
                consume_frames(files_chain[1:])
        finally:
            pass
       



def to_str(date):
    return date.strftime('%d_%m_%YT%H_%M_%S')
         


class Frames:
    def __init__(self, head, tail=None, duration=0):
        self.head = head
        self.tail = tail
        self.duration = duration
        self.length = 0
    
    def determine_end_date(self):
        frames_start_date_int, _, frames_end_date_int = unpack('=I4sI', self.tail.content[-16:-4])
       
        frames_end_date = datetime.datetime.fromtimestamp(frames_end_date_int, datetime.timezone.utc)
        frames_start_date = datetime.datetime.fromtimestamp(frames_start_date_int, datetime.timezone.utc)
        self.time_diff = frames_end_date - self.tail.date 
        if self.time_diff.seconds == 0:
            self.end_date = self.tail.date 
        else:
            self.end_date = frames_end_date 
            
        
    def write_to_file(self, base_path=OUT_PATH):
        head = self.head
        tail_frame = self.tail 
        
        self.fname = to_str(head.next.date) + " " + to_str(self.end_date)  + ".arv"
        
        self.channel = head.next.channel
    
        content = head.content
        frame = head
        subframes = 0
        while(frame.next):
            frame = frame.next
            
            if not self.channel.startswith("Cam")\
               and frame.channel.startswith("Cam"):
                self.channel = frame.channel
            subframes += 1
            content += frame.content
     
        if not os.path.exists(os.path.join(base_path, self.channel)):
            os.mkdir(os.path.join(base_path, self.channel))
        
        with open(os.path.join(base_path, self.channel, self.fname), 'wb') as f:
            f.write(content)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
            print("{} created frame {} length {} {}".format(now, self.fname, len(content), self.channel))
            if self.time_diff.seconds == 0:
                logging.info("{} created frame {} offset {} length {} {} subframes {}".format(now, self.fname, head.offset,
                                                                                            len(content), self.channel, subframes))
            else:
                logging.info("{} created frame {} offset {} length {} {} subframes {} **** {} secs".format(now, self.fname, head.offset, len(content), 
                                                                                                self.channel, subframes, self.time_diff.seconds ))

    def get_dates(self):
        return self.head.next.date, self.tail.date



class Frame:
    def __init__(self, raw, offset):
        self.signature, self._date, _, self.idx, _, self.length = unpack('=4sI8sI12sI',  raw)
        self.chunk_size = 36 + self.length
        self.channel = '-'
        self.offset = offset
        self.prev = None
        self.next = None
        
    @property
    def date(self):
         return datetime.datetime.fromtimestamp(self._date, datetime.timezone.utc)

    def is_valid(self):
        if self.signature == b'hFfE':
            return True 
        
        return False 
    
    def has_camera(self):
        if self.signature == b"Came":
            return True
        return False
           
    def is_first(self):
        if self.signature == b'hBfE': 
            return True

        return False
    
    def add_channel(self, buf):
        cam1, _, cam2 = unpack('=8s9s8s', buf)
        try:
            if cam1.startswith(b"Cam") and cam2.startswith(b"Cam"):
                self.channel = cam1.decode('ascii') + '_' + cam2.decode('ascii')
            elif cam1.startswith(b"Cam"):
                self.channel = cam1.decode('ascii')
            elif cam2.startwith(b"Cam"):
                self.channel = cam2.decode('ascii')
        except UnicodeDecodeError:
            self.channel = "Cam_ch_error"
            logging.error("Decode error on Cam")
            
            

class Parser:

    def __init__(self, filename):
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        self.read_co = self._read_data_co(filename)
        self.file_size = os.stat(filename).st_size


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


    def find_frames(self, offset=0, end_offset=None):
        if not end_offset:
            end_offset = self.file_size
            
        frames = None 
        previous_frame = None

        struct_fmt = '=4sI8sI12sI'
        total_length = 0
        struct_len = struct.calcsize(struct_fmt)
        while True:

            if offset + struct_len > end_offset:
                break
                
            next(self.read_co)         
            data = self.read_co.send((offset, struct_len))
            frame  = Frame(data, offset)
        
            if frame.is_first():
              #  print("located first frame at", frame.offset, frames)
              #  logging.info("located first frame at {}".format(frame.offset))
                if frames: # set size content for last frame
                    
                    previous_frame.chunk_size = frame.offset - previous_frame.offset
                 
                   # print("Setting last content", offset-previous_frame.chunk_size, offset, "prev Frame Offset", previous_frame.offset, "prev frame size", previous_frame.chunk_size)
                    next(self.read_co)
                    previous_frame.content = self.read_co.send((offset-previous_frame.chunk_size, previous_frame.chunk_size))
                    total_length += len(previous_frame.content)
                    
                    frames.length = total_length
                    frames.tail = previous_frame
                    yield frames, offset
                    total_length = 0
                    
                frames = Frames(frame)
              
                previous_frame = frame
                    
                offset += frame.chunk_size
            elif frame.has_camera():
                next(self.read_co)
                buffer_out = self.read_co.send((offset, 25))
            
                previous_frame.add_channel(buffer_out)
               
              #  logging.info("located channel {} {}".format(offset, previous_frame.channel))
                offset += 32
            elif frame.is_valid():  # valid frame based on signature        
              
                time_diff =  frame.date - previous_frame.date
                previous_frame.chunk_size = frame.offset - previous_frame.offset

                frame.prev = previous_frame
                previous_frame.next = frame
                next(self.read_co)
           
                previous_frame.content = self.read_co.send((offset-previous_frame.chunk_size, previous_frame.chunk_size))
                total_length += len(previous_frame.content)
                      #  print(offset, previous_frame.idx, previous_frame.chunk_size, 
                      #                                          previous_frame.offset, to_str(previous_frame.date),  previous_frame.channel, frame.channel)

              #  logging.info("set content {} {} {} {} {} {}".format(offset, previous_frame.idx, previous_frame.chunk_size, 
              #                                                  previous_frame.offset, to_str(previous_frame.date), 
              #                                                  previous_frame.channel))
                             

                offset += frame.chunk_size
                    

                previous_frame = frame 
            else:

                offset += 1



# threaded function 
def producer_frames(src_file, output_folder, start_offset, queue):
    parser = Parser(src_file)

    nof_frames = 0

    for frames in parser.find_frames(start_offset):
      frames.write_to_file(output_folder)
      queue.put((frames.fname, frames.channel))
          
      nof_frames += 1
    return nof_frames
        

# non  threaded function 
def produce_frames(src_file, output_folder, start_offset, end_offset):
    parser = Parser(src_file)

    all_frames = defaultdict(list)
    coverage = start_offset
    for frames, offset in parser.find_frames(start_offset, end_offset):
      coverage += frames.length
      
      print("progress {0:.2f}% coverage {0:.2f}%".format((offset/parser.file_size)*100, coverage/parser.file_size*100))
      frames.determine_end_date()
      frames.write_to_file(output_folder)
      all_frames[frames.channel].append(frames.get_dates())
    
    return all_frames


def merge_and_convert_frames_to_mp4(all_frames):
    files_chain = []
    
    for channel, dates in all_frames.items():
        dates.sort(key=lambda d:d[0])  # sort by date

        prev_date_start, prev_date_end = dates[0]
        
        files_chain.append(os.path.join(dst, channel, to_str(prev_date_start)+ " " + to_str(prev_date_end)+ ".arv"))
        logging.info("first frame joining {} number of joined frames {}".format(os.path.join(
                    dst, channel, to_str(prev_date_start)+ " " + to_str(prev_date_end)+ ".arv"), len(files_chain)))
      
        if len(dates) > 1:
           
            for idx, (date_start, date_end) in enumerate(dates[1:]):
                    time_diff = prev_date_end - date_start
                   
                    if  time_diff.seconds > 1 or len(files_chain) > MAX_FILE_HANDLES:#1 
                      
                        consume_frames(files_chain)
                        files_chain = []    
                   
                    files_chain.append(os.path.join(dst, channel, to_str(date_start)+ " " + to_str(date_end)+ ".arv"))  
                    logging.info("joining {} number of joined frames {}".
                                 format(os.path.join(dst, channel, to_str(date_start)+ " " + to_str(date_end)+ ".arv"), len(files_chain)))
                    prev_date_end = date_end   
        
        # only one arv non consecutive file


        consume_frames(files_chain)
            
        files_chain = []



def merge_and_convert_frames_to_mp4_from_files(dst):

    all_frames = defaultdict(list)
    for idx, (root, _, files) in  enumerate(os.walk(dst)):
        channel = os.path.split(root)[1]
     
        for file in files:  
            if file.endswith("arv"): #skip already created files
                date_start, date_end = file.split(".")[0].split(" ")
                
                all_frames[channel].append((datetime.datetime.strptime(date_start, '%d_%m_%YT%H_%M_%S'),
                                           datetime.datetime.strptime(date_end, '%d_%m_%YT%H_%M_%S')))
    
    merge_and_convert_frames_to_mp4(all_frames)
            

if __name__ == '__main__':
    
    src_file = sys.argv[1]
    dst = sys.argv[2]
    start_offset = int(sys.argv[3])
    if len(sys.argv)>4:
        end_offset = int(sys.argv[4])
    else:
        end_offset = 0
    # threaded ###########
    # queue = Queue()
    
    # for i in range(NTHREADS):
       # thread = Thread(target=consumer_frames, args=(sys.argv[2],))
       # thread.setDaemon(True)
       # thread.start()

    # nof_frames = producer_frames(src_file, dst, start_offset, queue)      
    # queue.join()
    
    #all_frames = produce_frames(src_file, dst, start_offset, end_offset)
   # merge_and_convert_frames_to_mp4(all_frames)
   # merge_and_convert_frames_to_mp4_from_files(dst)
    log_record_times_produce_srt(dst)