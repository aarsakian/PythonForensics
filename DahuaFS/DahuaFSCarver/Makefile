CC = gcc
CFLAGS = -static  -g -Wall 

TARGET = dhfs

BASEPATH = C:\Users\DevMan\Downloads

all: $(TARGET)

#INCLUDES = -I/usr/include/ffmpeg
INCLUDES =-I $(BASEPATH)\ffmpeg-4.2.2-win64-dev\include

LFLAGS = -L $(BASEPATH)\ffmpeg-4.2.2-win64-dev\lib -L $(BASEPATH)\ffmpeg-4.2.2-win64-dev\lib

LIBS = -lavcodec -Bstatic -lavformat -lavdevice -lavutil -lm

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) $(INCLUDES) -g  $(TARGET).c  -o $(TARGET) $(LFLAGS) $(LIBS)

clean: 
	$(RM) $(TARGET)

