#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "proj2.h"

#define MAX_KEYS 200
#define STATE_INVALID 0
#define STATE_BUSY    1
#define STATE_VALID   2

static struct {
    char name[32];
    int state;
} table[MAX_KEYS]; // database table

void handle_work(int fd) {
    // TODO: implement the server logic
}

int main(void) {
    // initialize the database
    for (int i = 0; i < MAX_KEYS; i++) {
        table[i].name[0] = '\0';
        table[i].state = STATE_INVALID;
    }

    // create a socket
    int port = 5000;
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    // convert the port to network byte order
    struct sockaddr_in server_address = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = 0
    };

    // bind the socket to the address
    if (bind(server_socket, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        perror("Cannot bind");
        exit(1);
    }

    // listen for incoming connections
    if (listen(server_socket, 2) < 0) {
        perror("Cannot listen");
        exit(1);
    }

    printf("Server listening on port 5000...\n");

    // blocked until a client connects
    while (1) {
        int fd = accept(server_socket, NULL, NULL);
        if (fd < 0) {
            perror("Cannot accept");
            exit(1);
        }

        handle_work(fd);

        close(fd);
    }
}