#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>

#define BLOCK_SIZE  1000000 //  1024*1024 
#define FRAME_START_ID "44484156"
#define SECONDS_DIFF 2
#define FIRST_FRAME_FLAG 253



typedef struct tm TimeFrame;

typedef struct Frame Frame;

typedef uint8_t BYTE; 

const BYTE True = 1;
const BYTE False = 0;


typedef struct {
  BYTE end_identifier[4];
  uint32_t tail;
} TailFrame;


typedef struct {
	BYTE start_identifier[4];
	//BYTE *content;
	unsigned short int type;
	unsigned short int channel;
	uint32_t number;
	uint32_t length; 
	TimeFrame time;
	
}HeaderFrame;



struct Frame {
	BYTE* content;
	BYTE is_corrupted; 
	uint32_t start;
	uint32_t end;
    HeaderFrame header;
    TailFrame tail;
    Frame* prev;
    Frame* next;
};


typedef struct{
  
    Frame head;
    Frame tail;
    double douration;
}Frames;

char* to_date(TimeFrame *);


void readChunk(char* fname,   BYTE* block_buf, uint32_t pos) {
	FILE * fp;

	fp = fopen(fname, "rb");
	
	fseek(fp, pos,  1);
	if (fp == NULL) {
	    fprintf(stderr, "open error for %s, errno = %d\n", fname, errno);
        exit(1);
	}
	fread(block_buf, BLOCK_SIZE + 1, 1,  fp);


	fclose(fp);
	
}

size_t getFileLen(char* fname){
	FILE * fp;
	fp = fopen(fname, "rb");
	fseek(fp, 0L, SEEK_END);
	
	size_t sz = ftell(fp);
	rewind(fp);
	return sz;
}


void replace(char* str, char oldchar, char newchar) {
	
	while(*str){
		if (*str == oldchar) {
			*str = newchar;
		} 
		
		str++;
	}
	
	
}

char* truncate_str(char* fname) {
	char* new_truncated;
	size_t pos = 1;
	while(fname++) {
		
		if(*fname=='\\') {
			break;
		}
		pos++;
	}
	memcpy(new_truncated, fname, pos);
	return new_truncated;
}



void write_frames(Frames* frames, char* path) {
	FILE *fp;
    Frame *frame = &frames->head;
    Frame *tail_frame = &frames->tail;
       
        //self.fname = to_str(frame.date()) + "_" + to_str(tail_frame.date()) + "____" + str(frame.start) + "----" + str(tail_frame.end) + ".dav"
      //  base_path = frame.header.get_path_using_channel(path)
	
	char fname[80];
	sprintf(fname, "%u----%u__", frame->start, tail_frame->end);

	char* s_name = to_date(&frame->header.time);
	replace(s_name, ':', '-');
    replace(s_name, ' ', '_');
	strncat(fname, s_name, strlen(s_name)-1);
	
	s_name = to_date(&tail_frame->header.time);
	replace(s_name, ':', '-');
    replace(s_name, ' ', '_');
	
	const char* delimiter = "__";
	strncat(fname, delimiter, strlen(delimiter));
	strncat(fname, s_name, strlen(s_name)-1);
	
	const char* extension = ".dhav";
	strncat(fname, extension, strlen(extension));
	
    BYTE* content = frame->content;
	
	fp = fopen(fname, "a+");
	if (fp == NULL) {
	    fprintf(stderr, "open error for %s, errno = %d\n", path, errno);
        exit(1);
	}
	
	uint32_t pos =0;
	
    while(frame) {
		while(pos<=frame->end - frame->start) {
			fputc(*(content+pos), fp);
			pos++;
			printf("%x",*(content+pos));
				
		}
		pos = 0;
        frame = frame->next;
		printf("new frame \n");
        content = frame->content;
	}
    //if not os.path.exists(base_path):
    //       os.mkdir(base_path)
        
       
         //   now = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
          //  print("{} created frame {} length {}".format(now, self.fname, len(content)))
           //logging.info("{} created frame {} length {}".format(now, self.fname, len(content)))

}



int is_frame_first(HeaderFrame *header) {
	if (header->type == FIRST_FRAME_FLAG) {
		return 1;
	}
	return 0;
	
}
TimeFrame convert_raw_time_to_dhfs_format(BYTE * raw_data) {
	uint32_t val;
	TimeFrame timeframe;
	memcpy(&val, raw_data, sizeof(uint32_t));
	//printf("%ld %x %x\n",val,(raw_data+1), (raw_data+2));
    timeframe.tm_year = 2000 + (val >> 26) - 1900; 
    timeframe.tm_mon = ((val & 62914560) >> 22) - 1;
    timeframe.tm_mday = (val & 4063232) >> 17;
    timeframe.tm_hour = (val  & 126976) >> 12;
    timeframe.tm_min = (val & 4032) >> 6;
    timeframe.tm_sec = (val & 63);
	timeframe.tm_isdst = -1;
	/*timeframe.tm_year = 2008-1900;
   timeframe.tm_mon = 1;
    timeframe.tm_mday = 4;
   timeframe.tm_hour = 02;
    timeframe.tm_min = 30;
    timeframe.tm_sec = 38;
    timeframe.tm_isdst = 0;*/
	return timeframe;
}

time_t to_time_t(TimeFrame * timeframe) {
	return mktime(timeframe);
	
}

char* to_date(TimeFrame * timeframe) {
	time_t tm = mktime(timeframe);
	return ctime(&tm);
	
	
}

int is_frame_whole(HeaderFrame* header, uint32_t offset){
	if ((header->length + offset) > BLOCK_SIZE) {
		return 0;
	} 
	return 1;
}

Frames parseFrames(BYTE* block_buf, uint32_t * pos) {
	
	uint8_t first_frame_found = 0;
	
	Frames frames;
	Frame* previous_frame;
	HeaderFrame headerframe;	
	TailFrame tailframe;
	TimeFrame timeframe;
	
	
	uint32_t rel_pos = 0;
	
 	while (rel_pos < BLOCK_SIZE)   {
		
		
		if (*(block_buf)== 68 && *(block_buf+1) == 72 && *(block_buf+2) == 65 && *(block_buf+3) == 86) { //DHAV
			
			memcpy(&headerframe, block_buf, sizeof(headerframe));
		
			if  (!is_frame_whole(&headerframe, rel_pos)) { //reached end of block
				block_buf++;
				rel_pos++;
				
				continue;
			}
			
			timeframe = convert_raw_time_to_dhfs_format(block_buf+16);
		  //  printf("FHEADER %d  y %d", timeframe.tm_mon, timeframe.tm_year);
		
			memcpy(&tailframe, block_buf + headerframe.length - sizeof(tailframe), sizeof(tailframe));
			headerframe.time = timeframe;
			Frame *frame = malloc(sizeof(Frame));
			frame->content = block_buf; 
			frame->is_corrupted = False;
			frame->start = *pos+rel_pos;
			frame->end = *pos+rel_pos+headerframe.length;
			frame->header = headerframe;
			frame->tail = tailframe;
			
			
			if (first_frame_found) {
					printf("found frame pos rel %d  abs pos %d\n", rel_pos, *pos);
				if ((difftime(to_time_t(&frame->header.time), to_time_t(&previous_frame->header.time)) < SECONDS_DIFF) && 
					(previous_frame->header.channel == frame->header.channel))
				{
				
					frame->prev = previous_frame;
					previous_frame->next = frame;
					
				} else {  // new sequence
					frames.tail = *previous_frame;
					(*pos) += rel_pos;
					printf("exiting time diff > 1 pos rel %d  abs pos %d\n", rel_pos, *pos);
					return frames;
				}
			} else { 
			
				if  (is_frame_first(&headerframe)) {
					first_frame_found = 1;
					frames.head = *frame;
					printf("starting frames sequence %d\n", rel_pos);
				}	else {
					block_buf++;
					rel_pos++;
					continue;
				}
			
			}
			previous_frame = frame;
			block_buf += headerframe.length;
			rel_pos += headerframe.length;
				
		} else {
			block_buf++;
			rel_pos++;
		
		}
		
	
	
	}
	//end of block reached 
	(*pos) += rel_pos;			
	return frames;
	
	
}

int main(int argc, char* argv[]) {
	FILE *fp;

	
	printf("about to read file");
	BYTE block_buf[BLOCK_SIZE] = {0};
	
	
	uint32_t file_len = getFileLen(argv[1]);
	uint32_t pos = 0;
	while (pos < file_len) {
		printf("new chunck %d %d file-lan\n", pos, file_len);
		readChunk(argv[1], block_buf,  pos);
		Frames frames = parseFrames(block_buf, &pos);
		write_frames(&frames, argv[2]);
	//	printf("pos %d", pos);

		
	}
	return 0;
}