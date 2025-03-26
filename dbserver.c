#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "proj2.h"

#define MAX_KEYS 200
#define BUFFER_LENGTH 4096
#define STATE_INVALID 0
#define STATE_BUSY    1
#define STATE_VALID   2

static int server_socket;

static pthread_mutex_t q_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t db_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_cond = PTHREAD_COND_INITIALIZER;

static int stats_writes = 0;
static int stats_reads  = 0;
static int stats_deletes = 0;
static int stats_fails  = 0;


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
} work_queue = {NULL, NULL}; // work queue

/*
 * Enqueues a new work item to the work queue.
 */
void enqueue_work(int fd) {

    pthread_mutex_lock(&q_lock);

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

    printf("Enqueue work: %d\n", fd);

    pthread_cond_signal(&q_cond);
    pthread_mutex_unlock(&q_lock);
}

/*
 * Dequeues a work item from the work queue.
 */
int dequeue_work(void) {
    pthread_mutex_lock(&q_lock);

    while (work_queue.head == NULL) {
        pthread_cond_wait(&q_cond, &q_lock);
    }

    struct work_item *item = work_queue.head;
    work_queue.head = item->next;
    if (work_queue.head == NULL) {
        work_queue.tail = NULL;
    }

    printf("Dequeue work: %d\n", item->fd);

    int fd = item->fd;
    free(item);

    pthread_mutex_unlock(&q_lock);
    return fd;
}

/*
 * Reads a fixed number of bytes from a file descriptor.
 */
int read_bytes(int fd, void *buf, int count) {
    int n;
    int bytes_read = 0;
    while (bytes_read < count) {
        n = read(fd, buf + bytes_read, count - bytes_read);
        if (n <= 0) {
            return 0;
        }
        bytes_read += n;
    }
    return 1;
}

/*
 * Writes a fixed number of bytes to a file descriptor.
 */
int write_bytes(int fd, void *buf, int count) {
    int n;
    int bytes_written = 0;
    while (bytes_written < count) {
        n = write(fd, buf + bytes_written, count - bytes_written);
        if (n <= 0) {
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

/*
 * Writes data to the database and stores it in a file.
 */
int do_write(const char *key_name, const char *data, int len) {

    pthread_mutex_lock(&db_lock);

    int idx = find_key_index(key_name);

    // if the key does not exist, find a free slot
    if (idx < 0) {
        idx = find_free_slot();
        if (idx < 0) {
            pthread_mutex_unlock(&db_lock);
            return 0;
        }
        strncpy(table[idx].name, key_name, sizeof(table[idx].name) - 1);
        table[idx].name[sizeof(table[idx].name) - 1] = '\0';
        table[idx].state = STATE_BUSY;
    } else {
        // if the key exists, check if it is busy
        if (table[idx].state == STATE_BUSY) {
            pthread_mutex_unlock(&db_lock);
            return 0;
        }
        table[idx].state = STATE_BUSY;
    }

    pthread_mutex_unlock(&db_lock);

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);
    if (!write_to_file(filename, data, len, idx)) {
        return 0;
    }

    // update the state of the key, lock the database
    pthread_mutex_lock(&db_lock);
    table[idx].state = STATE_VALID;
    pthread_mutex_unlock(&db_lock);
    return 1;
}

/*
 * Reads data from the database.
 */
int do_read(char *key_name, char *buf, int *length) {

    pthread_mutex_lock(&db_lock);

    int idx = find_key_index(key_name);
    if (idx < 0 || table[idx].state != STATE_VALID) {
        pthread_mutex_unlock(&db_lock);
        return 0;
    }

    pthread_mutex_unlock(&db_lock);

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);

    if (!read_from_file(filename, buf, BUFFER_LENGTH, idx)) {
        return 0;
    }

    // update the length of the data
    *length = strlen(buf);
    return 1;
}

/*
 * Deletes data from the database.
 */
int do_delete(const char *key_name) {

    pthread_mutex_lock(&db_lock);

    int idx = find_key_index(key_name);
    if (idx < 0 || table[idx].state != STATE_VALID) {
        pthread_mutex_unlock(&db_lock);
        return 0;
    }

    table[idx].state = STATE_INVALID;
    table[idx].name[0] = '\0';

    pthread_mutex_unlock(&db_lock);

    char filename[64];
    sprintf(filename, "/tmp/data.%d", idx);
    unlink(filename); // delete the file by unlinking it
    return 1;
}

/*
 * Finds the index of a key in the database.
 */
int find_key_index(const char *key_name) {
    for (int i = 0; i < MAX_KEYS; i++) {
        if (table[i].state == STATE_VALID && strcmp(table[i].name, key_name) == 0) {
            return i;
        }
    }
    return -1;
}

/*
 * Finds a free slot in the database.
 */
int find_free_slot(void) {
    for (int i = 0; i < MAX_KEYS; i++) {
        if (table[i].state == STATE_INVALID) {
            return i;
        }
    }
    return -1;
}

/*
 * Writes data to a file.
 */
int write_to_file(const char *filename, const char *data, int len, int idx) {

    pthread_mutex_lock(&db_lock);

    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0777);

    if (fd < 0) {
        perror("Cannot open file");
        table[idx].state = STATE_INVALID;
        pthread_mutex_unlock(&db_lock);
        return 0;
    }

    int n = write(fd, data, len);
    close(fd);

    if (n != len || n < 0) {
        perror("Cannot write to file");
        table[idx].state = STATE_INVALID;
        pthread_mutex_unlock(&db_lock);
        return 0;
    }

    pthread_mutex_unlock(&db_lock);

    return 1;
}

/*
 * Reads data from a file.
 */
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

    printf("read_from_file: read %d bytes. First few bytes: '%.*s'\n",
           n, n > 20 ? 20 : n, buf);

    if (n < len) {
        buf[n] = '\0';
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

    printf("Got request: op=%c name=%s len=%s\n",
           req.op_status, req.name, req.len);

    char op = req.op_status;
    int length = atoi(req.len);

    if (op == 'W') {
        stats_writes++;

        // check the length of the data
        if (length < 0 || length > BUFFER_LENGTH) {
            stats_fails++;
            res.op_status = 'X';
            write_bytes(fd, &res, sizeof(res)); // write error
            return;
        }

        // read the data from the client
        char buf[BUFFER_LENGTH];
        if (!read_bytes(fd, buf, length)) {
            stats_fails++;
            res.op_status = 'X';
            write_bytes(fd, &res, sizeof(res)); // write error
            return;
        }

        // write the data to the database
        res.op_status = do_write(req.name, buf, length) ? 'K' : 'X';
        write_bytes(fd, &res, sizeof(res));
        stats_fails += res.op_status == 'X';

        printf("Wrote %d bytes\n", length);
        printf("Response: op=%c\n", res.op_status);
    } else if (op == 'R') {
        stats_reads++;

        // read the data from the database
        char buf[BUFFER_LENGTH];
        res.op_status = do_read(req.name, buf, &length) ? 'K' : 'X';
        sprintf(res.len, "%d", length);
        write_bytes(fd, &res, sizeof(res));

        // send the data to the client only if the operation was successful
        if (res.op_status == 'K') {
            write_bytes(fd, buf, length);
        }
        stats_fails += res.op_status == 'X';

        printf("Read %d bytes\n", length);
        printf("Response: op=%c len=%s\n", res.op_status, res.len);
    } else if (op == 'D') {
        stats_deletes++;

        // delete the data from the database
        res.op_status = do_delete(req.name) ? 'K' : 'X';
        write_bytes(fd, &res, sizeof(res));
        stats_fails += res.op_status == 'X';

        printf("Deleted\n");
        printf("Response: op=%c\n", res.op_status);
    } else {
        // When the operation is invalid, increment the fails counter
        stats_fails++;
        res.op_status = 'X';
        write_bytes(fd, &res, sizeof(res));
        printf("Invalid operation\n");
        printf("Response: op=%c\n", res.op_status);
    }

}

void* listener_thread(void *arg) {
    while (1) {
        int fd = accept(server_socket, NULL, NULL);
        if (fd < 0) {
            perror("accept");
            continue;
        }
        printf("Listener thread running...\n");
        enqueue_work(fd);
    }
    return NULL;
}

void* worker_thread(void *arg) {
    while (1) {
        int fd = dequeue_work();
        handle_work(fd);
        printf("Worker thread running...\n");
        close(fd);
    }
    return NULL;
}

void print_stats() {
    int table_size = 0;
    pthread_mutex_lock(&db_lock);
    for (int i = 0; i < MAX_KEYS; i++) {
        if (table[i].state == STATE_VALID) {
            table_size++;
        }
    }
    pthread_mutex_unlock(&db_lock);

    int queue_size = 0;
    pthread_mutex_lock(&q_lock);
    struct work_item *item = work_queue.head;
    while (item) {
        queue_size++;
        item = item->next;
    }
    pthread_mutex_unlock(&q_lock);

    printf("Stats:\nwrites=%d\nreads=%d\ndeletes=%d\nfails=%d\ncurrent table size=%d\ncurrent queue size=%d\n",
           stats_writes, stats_reads, stats_deletes, stats_fails, table_size, queue_size);
}

int main(int argc, char **argv) {
    system("rm -f /tmp/data.*");

    // initialize the database
    for (int i = 0; i < MAX_KEYS; i++) {
        table[i].name[0] = '\0';
        table[i].state = STATE_INVALID;
    }

    // initialize the server socket and bind it to the port
    int port = 5000;
    if (argc > 1) {
        port = atoi(argv[1]);
        if (port <= 0) {
            fprintf(stderr, "Invalid port number: %s\n", argv[1]);
            exit(1);
        }
    }
    server_socket = socket(AF_INET, SOCK_STREAM, 0);

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

    printf("Server listening on port %d\n", port);

    // create the listener thread
    pthread_t lt;
    pthread_create(&lt, NULL, listener_thread, NULL);

    // create 4 worker threads
    pthread_t workers[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    // blocked until a client connects
    while (1) {
        char line[128];
        if (!fgets(line, sizeof(line), stdin)) {
            break;
        }

        if (strncmp(line, "quit", 4) == 0) {
            close(server_socket);
            exit(0);
        } else if (strncmp(line, "stats", 5) == 0) {
            print_stats();
        }
    }

    return  0;
}