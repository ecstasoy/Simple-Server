#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "proj2.h"

#define MAX_KEYS 200
#define BUFFER_LENGTH 4096
#define STATE_INVALID 0
#define STATE_BUSY    1
#define STATE_VALID   2

static struct {
    char name[32];
    int state;
} table[MAX_KEYS]; // database table

struct work_item {
    int fd;
    struct work_item *next;
};

static struct {
    struct work_item *head;
    struct work_item *tail;
} work_queue = {NULL, NULL};

void enqueue_work(int fd) {
    struct work_item *item = malloc(sizeof(*item));
    if (!item) {
        perror("malloc");
        exit(1);
    }
    item->fd = fd;
    item->next   = NULL;

    if (work_queue.tail) {
        work_queue.tail->next = item;
        work_queue.tail = item;
    } else {
        work_queue.head = item;
        work_queue.tail = item;
    }
}

int dequeue_work(void) {
    if (work_queue.head == NULL) {
        return -1;
    }

    struct work_item *item = work_queue.head;
    work_queue.head = item->next;
    if (work_queue.head == NULL) {
        work_queue.tail = NULL;
    }

    int fd = item->fd;
    free(item);
    return fd;
}

int read_bytes(int fd, void *buf, int count) {
    int n;
    int bytes_read = 0;
    while (bytes_read < count) {
        n = read(fd, buf + bytes_read, count - bytes_read);
        if (n < 0) {
            return 0;
        }
        bytes_read += n;
    }
    return 1;
}

int write_bytes(int fd, void *buf, int count) {
    int n;
    int bytes_written = 0;
    while (bytes_written < count) {
        n = write(fd, buf + bytes_written, count - bytes_written);
        if (n < 0) {
            return 0;
        }
        bytes_written += n;
    }
    return 1;
}

int find_key_index(const char *key_name);
int find_free_slot(void);

int write_to_file(const char *filename, const char *data, int len, int idx);
int read_from_file(const char *filename, char *buf, int len, int idx);

int do_write(const char *key_name, const char *data, int len) {

    int idx = find_key_index(key_name);

    if (idx < 0) {
        // free slot
        idx = find_free_slot();
        if (idx < 0) {
            return 0;
        }
        strncpy(table[idx].name, key_name, sizeof(table[idx].name) - 1);
        table[idx].name[sizeof(table[idx].name) - 1] = '\0';
        table[idx].state = STATE_BUSY;
    } else {
        // Overwriting exists in single-threaded environment
        table[idx].state = STATE_BUSY;
    }

    usleep(random() % 10000);

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);
    if (!write_to_file(filename, data, len, idx)) {
        return 0;
    }

    table[idx].state = STATE_VALID;
    return 1;
}

int do_read(char *key_name, char *buf, int *length) {

    int idx = find_key_index(key_name);
    if (idx < 0) {
        return 0;
    } else if (table[idx].state != STATE_VALID) {
        return 0;
    }

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);

    if (!read_from_file(filename, buf, *length, idx)) {
        return 0;
    }

    *length = strlen(buf);
    return 1;
}

int do_delete(const char *key_name) {

    int idx = find_key_index(key_name);
    if (idx < 0) {
        return 0;
    } else if (table[idx].state != STATE_VALID) {
        return 0;
    }

    table[idx].state = STATE_INVALID;
    table[idx].name[0] = '\0';

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);
    unlink(filename);

    return 1;
}

int find_key_index(const char *key_name) {
    for (int i = 0; i < MAX_KEYS; i++) {
        if (table[i].state == STATE_VALID && strcmp(table[i].name, key_name) == 0) {
            return i;
        }
    }
    return -1;
}

int find_free_slot(void) {
    for (int i = 0; i < MAX_KEYS; i++) {
        if (table[i].state == STATE_INVALID) {
            return i;
        }
    }
    return -1;
}

int write_to_file(const char *filename, const char *data, int len, int idx) {
    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0777);

    if (fd < 0) {
        perror("Cannot open file");
        table[idx].state = STATE_INVALID;
        return 0;
    }

    int n = write(fd, data, len);
    close(fd);

    if (n != len || n < 0) {
        perror("Cannot write to file");
        table[idx].state = STATE_INVALID;
        return 0;
    }

    return 1;
}

int read_from_file(const char *filename, char *buf, int len, int idx) {
    int fd = open(filename, O_RDONLY);

    if (fd < 0) {
        perror("Cannot open file");
        table[idx].state = STATE_INVALID;
        return 0;
    }

    int n = read(fd, buf, len);
    close(fd);

    if (n < 0) {
        perror("Cannot read from file");
        table[idx].state = STATE_INVALID;
        return 0;
    }

    return 1;
}

void handle_work(int fd) {
    struct request req;
    struct request res;

    if (!read_bytes(fd, &req, sizeof(req))) {
        res.op_status = 'X';
        write_bytes(fd, &res, sizeof(res)); // write error
        close(fd);
        return;
    }

    char op = req.op_status;
    int length = atoi(req.len);

    if (op == 'W') {
        if (length < 0 || length > BUFFER_LENGTH) {
            res.op_status = 'X';
            write_bytes(fd, &res, sizeof(res)); // write error
            return;
        }

        char buf[BUFFER_LENGTH];
        if (!read_bytes(fd, buf, length)) {
            res.op_status = 'X';
            write_bytes(fd, &res, sizeof(res)); // write error
            return;
        }

        res.op_status = do_write(req.name, buf, length) ? 'K' : 'X';
        write_bytes(fd, &res, sizeof(res));
    } else if (op == 'R') {
        char buf[BUFFER_LENGTH];
        res.op_status = do_read(req.name, buf, &length) ? 'K' : 'X';
        sprintf(res.len, "%d", length);
        write_bytes(fd, &res, sizeof(res));
        if (res.op_status == 'K') {
            write_bytes(fd, buf, length);
        }
    } else if (op == 'D') {
        res.op_status = do_delete(req.name) ? 'K' : 'X';
        write_bytes(fd, &res, sizeof(res));
    } else {
        res.op_status = 'X';
        write_bytes(fd, &res, sizeof(res));
    }

}

int main(void) {
    system("rm -f /tmp/data.*");

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

        enqueue_work(fd);

        while ((fd = dequeue_work()) >= 0) {
            handle_work(fd);
            close(fd);
        }
    }
}