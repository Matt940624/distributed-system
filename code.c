#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ctype.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>

#define BUFFER_SIZE 1024
#define PORT 8080
#define TOKEN_MSG 1
#define MARKER_MSG 2

struct message{
    int type;
    int snapshot_id;
    bool token;
};

struct channel{
    int sender_id;
    int receiver_id;
    bool is_closed;
    bool token_queue[BUFFER_SIZE];
    int queue_size;
};

struct snapshot_state
{
    int snapshot_id;
    int process_state;
    bool has_token;
    bool is_recording;
    struct channel *channels;
    int num_channels;
    int closed_channels;
};

struct state
{
    int id;
    int state;
    int preID;
    int sucID;
    struct snapshot_state *current_snapshot;
    bool is_snapshot_initiator;
};




// hostsfile
char *host_file_path;
// m
double marker_delay;
// t
double token_delay;
// s (state when process should initiate snapshot)
int state;
// p
int snapshot_id;
// starting
bool starting = false;

struct state initialState;
int num_hosts;
char **target_hosts;

// the passtoken for sending
bool token = true;

void get_hosts()
{

    FILE *host_file = fopen(host_file_path, "r");
    if (host_file == NULL)
    {
        perror("Failed to open host file");
        exit(EXIT_FAILURE);
    }

    char line[BUFFER_SIZE];
    num_hosts = 0;
    while (fgets(line, sizeof(line), host_file))
    {
        num_hosts++;
    }
    rewind(host_file);
    target_hosts = malloc(num_hosts * sizeof(char *));
    for (int i = 0; i < num_hosts; i++)
    {
        fgets(line, sizeof(line), host_file);
        line[strcspn(line, "\n")] = '\0';
        target_hosts[i] = strdup(line);
    }
    fclose(host_file);
}

int extract_number(const char *str)
{
    int num = 0;
    int found = 0;

    for (int i = 0; str[i] != '\0'; i++)
    {
        if (isdigit(str[i]))
        {
            num = num * 10 + (str[i] - '0');
            found = 1;
        }
    }
    return found ? num : -1;
}

void initialize_state()
{
    char hostname[BUFFER_SIZE];
    get_hosts();
    if (gethostname(hostname, sizeof(hostname)) != 0)
    {
        perror("gethostname failed");
        exit(EXIT_FAILURE);
    }

    int extracted_id = extract_number(hostname);
    if (extracted_id == -1)
    {
        perror("Failed to extract ID from hostname");
        exit(EXIT_FAILURE);
    }

    initialState.id = extracted_id;

    if (starting)
    {
        initialState.state = 1;
    }
    else
    {
        initialState.state = 0;
    }

    if (initialState.id == 1)
    {
        initialState.preID = num_hosts;
        initialState.sucID = 2;
    }
    else if (initialState.id == num_hosts)
    {
        initialState.preID = num_hosts - 1;
        initialState.sucID = 1;
    }
    else
    {
        initialState.preID = initialState.id - 1;
        initialState.sucID = initialState.id + 1;
    }
}

int is_number(const char *str, double *out_value)
{
    if (str == NULL || *str == '\0')
        return 0;
    char *endptr;
    *out_value = strtod(str, &endptr);
    return (*endptr == '\0');
}

char *resolve_hostnames(int peerid)
{
    char hostname[BUFFER_SIZE];
    snprintf(hostname, sizeof(hostname), "peer%d", peerid);
    return strdup(hostname);
}

void init_snapshot(struct state *proc_state, int snapshot_id) {
    proc_state->current_snapshot = malloc(sizeof(struct snapshot_state));
    proc_state->current_snapshot->snapshot_id = snapshot_id;
    proc_state->current_snapshot->process_state = proc_state->state;
    proc_state->current_snapshot->has_token = token;
    proc_state->current_snapshot->is_recording = true;
    proc_state->current_snapshot->closed_channels = 0;
    
    // Initialize channels
    proc_state->current_snapshot->num_channels = num_hosts - 1;
    proc_state->current_snapshot->channels = malloc(sizeof(struct channel) * (num_hosts - 1));
    
    int channel_idx = 0;
    for (int i = 1; i <= num_hosts; i++) {
        if (i != proc_state->id) {
            proc_state->current_snapshot->channels[channel_idx].sender_id = i;
            proc_state->current_snapshot->channels[channel_idx].receiver_id = proc_state->id;
            proc_state->current_snapshot->channels[channel_idx].is_closed = false;
            proc_state->current_snapshot->channels[channel_idx].queue_size = 0;
            channel_idx++;
        }
    }
}

int create_connection(const char *hostname) {
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    struct hostent *he = gethostbyname(hostname);
    if (he == NULL) {
        fprintf(stderr, "Could not resolve hostname %s\n", hostname);
        close(sock);
        return -1;
    }

    memcpy(&serv_addr.sin_addr, he->h_addr_list[0], he->h_length);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection failed");
        close(sock);
        return -1;
    }

    return sock;
}

void send_marker(struct state *proc_state) {
    struct message marker_msg;
    marker_msg.type = MARKER_MSG;
    marker_msg.snapshot_id = proc_state->current_snapshot->snapshot_id;
    
    for (int i = 1; i <= num_hosts; i++) {
        if (i != proc_state->id) {
            char *peer = resolve_hostnames(i);
            int sock = create_connection(peer);
            if (sock >= 0) {
                send(sock, &marker_msg, sizeof(marker_msg), 0);
                fprintf(stderr, "{proc_id:%d, snapshot_id:%d, sender:%d, receiver:%d, msg:\"marker\", state:%d, has_token:%s}\n",
                    proc_state->id, marker_msg.snapshot_id, proc_state->id, i, 
                    proc_state->state, token ? "YES" : "NO");
                close(sock);
            }
            free(peer);
        }
    }
}

void handle_marker(struct state *proc_state, struct message *msg, int sender_id) {
    if (proc_state->current_snapshot == NULL) {
        // First marker received
        fprintf(stderr, "{proc_id:%d, snapshot_id:%d, snapshot:\"started\"}\n",
            proc_state->id, msg->snapshot_id);
            
        init_snapshot(proc_state, msg->snapshot_id);
        
        // Close the channel from sender
        for (int i = 0; i < proc_state->current_snapshot->num_channels; i++) {
            if (proc_state->current_snapshot->channels[i].sender_id == sender_id) {
                proc_state->current_snapshot->channels[i].is_closed = true;
                proc_state->current_snapshot->closed_channels++;
                break;
            }
        }
        
        // Wait for marker delay before sending markers
        sleep(marker_delay);
        send_marker(proc_state);
    } else {
        // Subsequent marker
        for (int i = 0; i < proc_state->current_snapshot->num_channels; i++) {
            if (proc_state->current_snapshot->channels[i].sender_id == sender_id) {
                proc_state->current_snapshot->channels[i].is_closed = true;
                proc_state->current_snapshot->closed_channels++;
                
                // Print channel state
                fprintf(stderr, "{proc_id:%d, snapshot_id:%d, snapshot:\"channel closed\", channel:\"%d-%d\", queue:[",
                    proc_state->id, msg->snapshot_id, sender_id, proc_state->id);
                
                for (int j = 0; j < proc_state->current_snapshot->channels[i].queue_size; j++) {
                    fprintf(stderr, "%s%s", j > 0 ? "," : "", 
                        proc_state->current_snapshot->channels[i].token_queue[j] ? "token" : "");
                }
                fprintf(stderr, "]}\n");
                
                break;
            }
        }
        
        // Check if snapshot is complete
        if (proc_state->current_snapshot->closed_channels == proc_state->current_snapshot->num_channels) {
            fprintf(stderr, "{proc_id:%d, snapshot_id:%d, snapshot:\"complete\"}\n",
                proc_state->id, msg->snapshot_id);
            
            // Clean up snapshot
            free(proc_state->current_snapshot->channels);
            free(proc_state->current_snapshot);
            proc_state->current_snapshot = NULL;
        }
    }
}

void send_token()
{
    char *next_peer = resolve_hostnames(initialState.sucID);
    if (!next_peer)
    {
        fprintf(stderr, "Failed to resolve successor's hostname\n");
        return;
    }

    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket creation failed");
        free(next_peer);
        return;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    struct hostent *he = gethostbyname(next_peer);
    if (he == NULL)
    {
        fprintf(stderr, "Could not resolve hostname %s\n", next_peer);
        free(next_peer);
        close(sock);
        return;
    }

    memcpy(&serv_addr.sin_addr, he->h_addr_list[0], he->h_length);

    while (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("Connection failed, retrying...");
        sleep(1);
    }

    struct message token_msg;
    token_msg.type = TOKEN_MSG;
    token_msg.token = token;
    // The Token is defined using a boolean
    send(sock, &token_msg, sizeof(token_msg), 0);

    fprintf(stderr, "{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
            initialState.id, initialState.id, initialState.sucID);

    close(sock);
    free(next_peer);
}

void *start_server()
{
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1, addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)))
    {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0)
    {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    

    while (1)
    {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0)
        {
            perror("Accept failed");
            exit(EXIT_FAILURE);
        }

        struct message msg;
        ssize_t bytes_received = read(new_socket, &msg, sizeof(msg));
        if (bytes_received <= 0)
        {
            perror("Read failed or connection closed");
            close(new_socket);
            continue;
        }
        if (msg.type == MARKER_MSG)
        {
            handle_marker(&initialState, &msg, msg.snapshot_id);
        }
        else if (msg.type == TOKEN_MSG)
        {
            // Handle token message
            token = msg.token;
            initialState.state++;

            // Record token in snapshot if recording
            if (initialState.current_snapshot != NULL) {
                for (int i = 0; i < initialState.current_snapshot->num_channels; i++) {
                    if (!initialState.current_snapshot->channels[i].is_closed) {
                        initialState.current_snapshot->channels[i].token_queue[
                            initialState.current_snapshot->channels[i].queue_size++] = true;
                    }
                }
            }

            fprintf(stderr, "{proc_id: %d, state: %d}\n", initialState.id, initialState.state);
            fprintf(stderr, "{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
                    initialState.id, initialState.preID, initialState.id);

            // Check if we should initiate snapshot
            if (state > 0 && initialState.state == state) {
                fprintf(stderr, "{proc_id:%d, snapshot_id:%d, snapshot:\"started\"}\n",
                    initialState.id, snapshot_id);
                init_snapshot(&initialState, snapshot_id);
                sleep(marker_delay);
                send_marker(&initialState);
            }

            sleep(token_delay);
            send_token();
        
    }
        close(new_socket);
    }
}

void parsingArguments(int argc, char *argv[])
{

    
    // Parsing command-line arguments
    int opt;
    while ((opt = getopt(argc, argv, "h:m:t:s:p:x")) != -1)
    {
        switch (opt)
        {
        case 'h':
            host_file_path = optarg; // Get the file name
            break;
        case 'm':
            marker_delay = atoi(optarg); // Convert string to integer
            break;
        case 't':
            token_delay = atof(optarg); // Convert string to float
            break;
        case 's':
            state = atoi(optarg);
            break;
        case 'p':
            snapshot_id = atoi(optarg);
            break;
        case 'x':
            starting = true; // Boolean flag (presence means enabled)
            break;
        default:
            fprintf(stderr, "Usage: %s [-h hostsfile] [-m value] [-t time] [-s value] [-p value] [-x]\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }


}

int main(int argc, char const *argv[])
{
    parsingArguments(argc, argv);
    initialize_state();

    fprintf(stderr, "{proc_id: %d, state: %d, predecessor: %d, successor: %d}\n",
            initialState.id, initialState.state, initialState.preID, initialState.sucID);

    if (fork() == 0)
    {
        start_server();
    }

    if (starting)
    {
        sleep(token_delay);
        send_token();
    }

    while (1)
        sleep(1);
}
