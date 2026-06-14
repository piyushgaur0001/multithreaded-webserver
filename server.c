#include "proxy_parse.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/wait.h>
#include <fcntl.h>

typedef struct cache_element cache_element;

#define MAX_CLIENTS 10
#define MAX_BYTES 4096

struct cache_element {
    char *data;
    int len;
    char *url;
    time_t lru_time_track;
    cache_element *next;
};

cache_element *find(char *url);

int add_cache_element(char *data, int size, char *url);

void remove_cache_element();

int port_number = 8080;

int proxy_socketId;
pthread_t tid[MAX_CLIENTS];

sem_t semaphore;
pthread_mutex_t lock;

cache_element *head;
int cache_size;

int connectRemoteServer(char *host_addr, int port_num){
    int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(remoteSocket < 0){
        printf("Error creating socket to connect to remote server\n");
        return -1;
    }

    struct hostent *host = gethostbyname(host_addr);
    if(host == NULL){
        printf("No such host exists \n");
        return -1;
    }

    struct sockaddr_in remote_addr;
    bzero((char *)&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(port_num);

    bcopy((char *)&host->h_addr_list[0], (char *)&remote_addr.sin_addr.s_addr, host->h_length);

    if(connect(remoteSocket, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0){
        printf("Error connecting to remote server\n");
        return -1;
    }
    return remoteSocket;
}

int handle_request(int socket, ParsedRequest *request, char *tempReq){
    char *buf = (char *)malloc(MAX_BYTES*sizeof(char));
    strcpy(buf, "GET ");
    strcat(buf, request->path);
    strcat(buf, " ");
    strcat(buf, request->version);
    strcat(buf, "\r\nHost: ");

    size_t len = strlen(buf);

    if(ParsedHeader_set(request, "Connection", "close") < 0){
        perror("Error setting header");
    }

    if(ParsedHeader_get(request, "Host") == NULL){
        if(ParsedHeader_set(request, "Host", request->host) < 0){
            perror("Error setting header");
        }
    }

    if(ParsedRequest_unparse_headers(request, buf+len, (size_t)(MAX_BYTES-len)) < 0){
        perror("Error unparsing headers");
    }

    int server_port = 80;
    if(request->port != NULL){
        server_port = atoi(request->port);
    }

    int remoteSocketId = connectRemoteServer(request->host, server_port);
}

void *handle_client(void *socketNew){
    sem_wait(&semaphore);
    int p;
    sem_getvalue(&semaphore, &p);
    printf("Semaphore value is: %d\n", p);

    int *t = (int *)socketNew;
    int socket = *t;

    int bytes_send_client, len;

    char *buffer = (char *)calloc(MAX_BYTES, sizeof(char));
    bzero(buffer, MAX_BYTES);
    bytes_send_client = recv(socket, buffer, MAX_BYTES, 0);

    while(bytes_send_client > 0){
        len = strlen(buffer);
        if(strstr(buffer, "\r\n\r\n") == NULL){
            bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
        }
        else{
            break;
        }
    }

    char *tempReq = (char *)malloc(strlen(buffer)*sizeof(char)+1);
    for(int i=0; i<strlen(buffer); i++){
        tempReq[i] = buffer[i];
    }

    struct cache_element *temp = find(tempReq);
    if(temp != NULL){
        int size = temp->len/sizeof(char);
        int pos = 0;
        char response[MAX_BYTES];
        while(pos < size){
            bzero(response, MAX_BYTES);
            for(int i=0; i<MAX_BYTES;i++){
                response[i] = temp->data[i];
                pos++;
            }
            send(socket, response, MAX_BYTES, 0);
        }
        printf("Date retrieved from cache\n");
        printf("%s\n\n",response);  
    }
    else if(bytes_send_client > 0 ){
        len = strlen(buffer);
        ParsedRequest *request = ParsedRequest_create();

        if(ParsedRequest_parse(request, buffer, len) < 0){
            perror("Error parsing request");
        }
        else{
            bzero(buffer, MAX_BYTES);
            if(!strcmp(request->method, "GET")){
                if(request->host && request-> path && checkHTTPversion(request->version) == 1){
                    bytes_send_client = handle_request(socket, request, tempReq);
                    if(bytes_send_client == -1 ){
                        sendErrorMessage(socket, 500);
                    }
                }
                else{
                    sendErrorMessage(socket, 500);
                }
            }
            else{
                printf("Method not supported\n");
            }
        }
        ParsedRequest_destroy(request);
    }
    else if(bytes_send_client == 0){
        printf("Client closed the connection\n");
    }
    shutdown(socket, SHUT_RDWR);
    close(socket);
    free(buffer);
    sem_post(&semaphore);
    sem_getvalue(&semaphore, &p);
    printf("Semaphore value is: %d\n", p);
    free(tempReq);
}

int main(int argc, char *argv[]){
    int client_socketId, client_len;
    struct sockaddr_in server_addr, client_addr;

    sem_init(&semaphore, 0, MAX_CLIENTS);
    pthread_mutex_init(&lock, NULL);

    if(argv == 2){
        port_number = atoi(argv[1]);
    }
    else{
        printf("Too few arguments\n");
        exit(1);
    }

    printf("Starting proxy server on port %d\n", port_number);

    proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);
    if(proxy_socketId < 0){
        perror("Error creating socket");
        exit(1);
    }

    int reuse = 1;
    if(setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0){
        perror("Error setting socket options");
        exit(1);
    }

    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_number);

    if(bind(proxy_socketId, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        perror("Error binding socket");
        exit(1);
    }
    printf("Binding on port %d successful\n", port_number);

    int listen_status = listen(proxy_socketId, MAX_CLIENTS);
    if(listen_status < 0){
        perror("Error listening on socket");
        exit(1);
    }

    int i=0;
    int connectedSocketId[MAX_CLIENTS];
    while(1){
        bzero((char *)&client_addr, sizeof(client_addr));
        client_len = sizeof(client_addr);
        client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr, &client_len);
        if(client_socketId < 0){
            perror("Error accepting connection");
            continue;
        }
        else{
            connectedSocketId[i] = client_socketId;
        }

        struct sockaddr_in *client_ip = (struct sockaddr_in *)&client_addr;
        struct in_addr client_in_addr = client_ip->sin_addr;
        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_in_addr, str, INET_ADDRSTRLEN);
        printf("Client is connected with port number : %d and IP address : %s\n", ntohs(client_ip->sin_port), str);

        pthread_create(&tid[i], NULL, handle_client, (void *)&connectedSocketId[i]);
        i++;
    }
    close(proxy_socketId);
    return 0;

}

