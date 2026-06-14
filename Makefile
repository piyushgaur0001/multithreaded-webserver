CC=g++
CFLAGS=-Wall

all: proxy

proxy:
	$(CC) -c $(CFLAGS) proxy_parse.c -o proxy_parse.o
	$(CC) -c $(CFLAGS) proxy_server.c -o proxy.o
	$(CC) proxy.o proxy_parse.o -o proxy -lpthread

clean:
	rm -f proxy *.o