#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
//#include <libavutil/timestamp.h>
#include <libavformat/avformat.h>


#define BLOCK_SIZE  1000000 //  1024*1024 
#define FRAME_START_ID "44484156"
#define SECONDS_DIFF 2
#define FIRST_FRAME_FLAG 253



typedef struct tm TimeFrame;

typedef struct Frame Frame;

typedef uint8_t BYTE; 

const BYTE True = 1;
const BYTE False = 0;


void convert_dhav_to_mp4(const char* in_filename, const char* out_filename){
	AVFormatContext *input_format_context = NULL;
	AVFormatContext *output_format_context = NULL;	
	int ret;
	int *streams_list = NULL;
	if ((ret = avformat_open_input(&input_format_context, in_filename, NULL, NULL)) < 0) {
		fprintf(stderr, "Could not open input file '%s'", in_filename);
		abort();
	}
	if ((ret = avformat_find_stream_info(input_format_context, NULL)) < 0) {
		fprintf(stderr, "Failed to retrieve input stream information");
		abort();
	}

	avformat_alloc_output_context2(&output_format_context, NULL, NULL, out_filename);
	if (!output_format_context) {
		fprintf(stderr, "Could not create output context\n");
		ret = AVERROR_UNKNOWN;
		abort();
	}
	
	unsigned int number_of_streams = input_format_context->nb_streams;
	streams_list = av_mallocz_array(number_of_streams, sizeof(*streams_list));
		
}

typedef struct {
  BYTE end_identifier[4];
  uint32_t tail;
} TailFrame;


typedef struct {
	BYTE start_identifier[4];
	//BYTE *content;
	uint16_t type;
	uint16_t  channel;
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
  
    Frame* head;
    Frame* tail;
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
    Frame *frame = frames->head;
    Frame *tail_frame = frames->tail;
       
        //self.fname = to_str(frame.date()) + "_" + to_str(tail_frame.date()) + "____" + str(frame.start) + "----" + str(tail_frame.end) + ".dav"
      //  base_path = frame.header.get_path_using_channel(path)
	
	char fname[80];
	

	char* s_name = to_date(&frame->header.time);
	replace(s_name, ':', '-');
    replace(s_name, ' ', '_');
	strcpy(fname, s_name);
	
	strtok(fname, "\n"); //cut off at new line delimiter
	
	s_name = to_date(&tail_frame->header.time);
	replace(s_name, ':', '-');
    replace(s_name, ' ', '_');
	
	const char* delimiter = "__";
	strncat(fname, delimiter, strlen(delimiter));
	strncat(fname, s_name, strlen(s_name)-1);
		
	char offsets[50];
	sprintf(offsets, "_%u----%u", frame->start, tail_frame->end);
	
	strncat(fname, offsets, strlen(offsets));
	
	const char* extension = ".dhav";
	strncat(fname, extension, strlen(extension));
	
    BYTE* content = frame->content;
	
	
	char channel[5];
	sprintf(channel, "%u", frame->header.channel+1);
	
	char curdir[100];
	
	getcwd(curdir, sizeof(curdir));
	
	if (mkdir(channel) == -1) {
		fprintf(stderr, "directory creating error errno = %d\n", errno);
		
	}
	
	if (chdir(channel) == -1) {
		fprintf(stderr, "error changing directory errno = %d\n", errno);
		exit(1);
	}
	
	
	fp = fopen(fname, "wb+");
	if (fp == NULL) {
	    fprintf(stderr, "open error for  errno = %d\n",  errno);
        exit(1);
	}
	
	
		
	uint32_t pos =0;
	uint32_t noframes = 0;

	
    for(;;){
		noframes++;
	
		fwrite(content, frame->header.length, 1, fp);
		pos += frame->end - frame->start;

	//	printf("new frame %d -> %d \n", frame->start, frame->end);
		if (frame->end==tail_frame->end) {
			break;
		}
        frame = frame->next;
		
        content = frame->content;
	}
	printf("total %d\n", pos);
	fclose(fp);
	chdir(curdir);
}

int is_frame_corrupted(HeaderFrame *header) {
	
	if (header->time.tm_hour>23 || header->time.tm_hour < 0 ||
	    header->time.tm_min<0 || header->time.tm_min>59 || 
		header->time.tm_sec<0 || header->time.tm_sec>59 ||
		header->time.tm_mon<0 || header->time.tm_mon > 11) {
			return True;
		}
	
	return False;
	
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

int frame_has_not_legit_channel(HeaderFrame* header) {
	
	if (header->channel >15) {
		return 1;
	} 
	return 0;
	
}

int is_frame_whole(HeaderFrame* header, uint32_t offset){
	if ((header->length + offset) > BLOCK_SIZE) {
		return 0;
	} 
	return 1;
}

int parseFrames(Frames* frames, BYTE* block_buf, uint32_t * pos, uint8_t *first_frame_found) {
	
	
	
	
	Frame* previous_frame;
	HeaderFrame headerframe;	
	TailFrame tailframe;
	TimeFrame timeframe;
	
	if (first_frame_found) {  //rebind
		previous_frame = frames->tail;
		
	}
	
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
			
			if (is_frame_corrupted(&headerframe)) {
				rel_pos += headerframe.length;
				continue;
			}
			
			if (frame_has_not_legit_channel(&headerframe)) {
				rel_pos += headerframe.length;
				continue;
			}
			
			Frame *frame = malloc(sizeof(Frame));
			frame->content = block_buf; 
			frame->is_corrupted = False;
			frame->start = *pos+rel_pos;
			frame->end = *pos+rel_pos+headerframe.length;
			frame->header = headerframe;
			frame->tail = tailframe;
			
			
			if (*first_frame_found) {
				//	printf("found frame pos rel %d  abs pos %d\n", rel_pos, *pos);
				if ((difftime(to_time_t(&frame->header.time), to_time_t(&previous_frame->header.time)) < SECONDS_DIFF) && 
					(previous_frame->header.channel == frame->header.channel))
				{
				
					frame->prev = previous_frame;
					previous_frame->next = frame;
					
				} else {  // new sequence
					frames->tail = previous_frame;
					(*pos) = frames->tail->end;
					//printf("exiting time diff > 1 pos rel %d  abs pos %d\n", rel_pos, *pos);
					return 1;
				}
			} else { 
			
				if  (is_frame_first(&headerframe)) {
					*first_frame_found = 1;
					frames->head = frame;
				//	printf("starting frames sequence %d\n", rel_pos);
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
	//end of block reached  termination because buffer has been exhausted not time diff
	if (rel_pos == BLOCK_SIZE) //did not locate any frame at all
		(*pos) += BLOCK_SIZE;
	else {
		(*pos) = previous_frame->end;
	}
	
	frames->tail = previous_frame; //use it only when we find out that the first frame of next block is not successor
	return 0;
	
	
}

int main(int argc, char* argv[]) {

	//printf("about to read file");
	BYTE block_buf[BLOCK_SIZE] = {0};
	
	uint8_t is_chain_complete;
	
	uint32_t file_len = getFileLen(argv[1]);
	uint32_t pos = 0;
	uint8_t first_frame_found = 0;
	Frames* frames = malloc(sizeof(frames));
	
	while (pos < file_len) {
		printf("new chunck %d %d file-lan\n", pos, file_len);
		readChunk(argv[1], block_buf,  pos);
		is_chain_complete = parseFrames(frames, block_buf, &pos, &first_frame_found);
		if (is_chain_complete) {
			printf("moved pos %d \n", pos);
			write_frames(frames, argv[2]);
			first_frame_found = 0;
		} 

		
	}
	return 0;
}