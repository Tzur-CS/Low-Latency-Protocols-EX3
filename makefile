all: server client

#server: client_server.c db_helper.h client.h
#	gcc client_server.c db_helper.h client.h -o server -libverbs
server: client_server.c map.h map.c linkedlist.h linkedlist.c
	gcc client_server.c map.h map.c linkedlist.h linkedlist.c -o server -libverbs

client:
	ln -s server client



