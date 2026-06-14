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
#define MAX_ELEMENT_SIZE 1048576
#define MAX_SIZE 10485760

int port_number = 8080;

int proxy_socketId;
pthread_t tid[MAX_CLIENTS];

sem_t semaphore;
pthread_mutex_t lock;

cache_element *head;
int cache_size;

struct cache_element {
    char *data;
    int len;
    char *url;
    time_t lru_time_track;
    cache_element *next;
};

cache_element *find(char *url){
    cache_element *site = NULL;
    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Remove cache lock acquired: %d\n", temp_lock_val);
    if(head !=NULL){
        site = head;
        while(site != NULL){
            if(strcmp(site->url, url) == 0){
                printf("LRU time track before: %ld\n", site->lru_time_track);
                printf("Cache hit for url: %s\n", url);
                site->lru_time_track = time(NULL);
                printf("LRU time track after: %ld\n", site->lru_time_track);
                break;
            }
            site = site->next;
        }
    }else{
        printf("Cache is empty\n");
    }

    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("Remove cache lock released: %d\n", temp_lock_val);
    return site;
}

void remove_cache_element(){
    cache_element *p;
    cache_element *q;
    cache_element *temp;

    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Remove cache lock acquired: %d\n", temp_lock_val);
    if(head != NULL){
        for(q=head, p=head, temp=head; q->next !=NULL; q=q->next){
            if((q->next->lru_time_track) < temp->lru_time_track){
                temp = q->next;
                p = q;
            }
        }
        if(temp==head){
            head = head->next;
        }
        else{
            p->next = temp->next;
        }
        cache_size -= (temp->len)+ sizeof(cache_element) +strlen(temp->url) + 1;
        free(temp->data);
        free(temp->url);
        free(temp);
    }
    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("Remove cache lock released: %d\n", temp_lock_val);
    
}

int add_cache_element(char *data, int size, char *url){
    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Add cache lock acquired: %d\n", temp_lock_val);
    int element_size = size+1+strlen(url)+sizeof(cache_element);
    if(element_size > MAX_ELEMENT_SIZE){
        temp_lock_val = pthread_mutex_unlock(&lock);
        printf("Add cache lock released: %d\n", temp_lock_val);
        return 0;
    }
    else{
        while(cache_size + element_size > MAX_SIZE){
            remove_cache_element();
        }
        cache_element *element = (cache_element *)malloc(sizeof(cache_element));
        element->data = (char *)malloc(size+1);
        strcpy(element->data, data);
        element->url = (char *)malloc(strlen(url)+1);
        strcpy(element->url, url);
        element->lru_time_track = time(NULL);
        element->next = head;
        element->len = size;
        head = element;
        cache_size += element_size;
        temp_lock_val = pthread_mutex_unlock(&lock);
        printf("Add cache lock released: %d\n", temp_lock_val);
        return 1;
    }
    return 0;
}

int sendErrorMessage(int socket, int errorCode){
    char *message;
    if(errorCode == 500){
        message = "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/html\r\n\r\n<html><body><h1>500 Internal Server Error</h1></body></html>";
    }
    else if(errorCode == 404){
        message = "HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\n\r\n<html><body><h1>404 Not Found</h1></body></html>";
    }
    else{
        message = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html\r\n\r\n<html><body><h1>400 Bad Request</h1></body></html>";
    }
    return send(socket, message, strlen(message), 0);
}

int checkHTTPversion(char *version){
    if(strcmp(version, "HTTP/1.0") == 0 || strcmp(version, "HTTP/1.1") == 0){
        return 1;
    }
    return 0;
}

int connectRemoteServer(char *host_addr, int port_num){
    printf("Connecting to host: %s port: %d\n", host_addr, port_num);

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[6];
    snprintf(port_str, sizeof(port_str), "%d", port_num);

    if(getaddrinfo(host_addr, port_str, &hints, &res) != 0){
        perror("getaddrinfo failed");
        return -1;
    }

    int remoteSocket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if(remoteSocket < 0){
        perror("socket failed");
        freeaddrinfo(res);
        return -1;
    }

    if(connect(remoteSocket, res->ai_addr, res->ai_addrlen) < 0){
        perror("connect failed");
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);
    return remoteSocket;
}

int handle_request(int clientSocketId, ParsedRequest *request, char *tempReq){
    char *buf = (char *)malloc(MAX_BYTES*sizeof(char));
    strcpy(buf, "GET ");
    strcat(buf, request->path);
    strcat(buf, " ");
    strcat(buf, request->version);
    strcat(buf, "\r\n");

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
    if(remoteSocketId < 0){
        return -1;
    }

    int bytes_send = send(remoteSocketId, buf, strlen(buf), 0);
    bzero(buf, MAX_BYTES);

    bytes_send = recv(remoteSocketId, buf, MAX_BYTES-1, 0);

    char *temp_buffer = (char *)malloc(MAX_BYTES*sizeof(char));
    int temp_buffer_size = MAX_BYTES;
    int temp_buffer_index = 0;

    while(bytes_send > 0){
        send(clientSocketId, buf, bytes_send, 0);
        for(int i = 0; i < bytes_send; i++){
            temp_buffer[temp_buffer_index++] = buf[i];
            if(temp_buffer_index == temp_buffer_size){
                temp_buffer_size *= 2;
                temp_buffer = (char *)realloc(temp_buffer, temp_buffer_size);
            }
        }
        bzero(buf, MAX_BYTES);
        bytes_send = recv(remoteSocketId, buf, MAX_BYTES-1, 0);
    }
    temp_buffer[temp_buffer_index] = '\0';
    add_cache_element(temp_buffer, temp_buffer_index, tempReq);
    free(temp_buffer);
    free(buf);
    close(remoteSocketId);
    return 0;

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
    return NULL;
}

int main(int argc, char *argv[]){
    int client_socketId;
    socklen_t client_len;
    struct sockaddr_in server_addr, client_addr;

    sem_init(&semaphore, 0, MAX_CLIENTS);
    pthread_mutex_init(&lock, NULL);

    if(argc == 2){
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

