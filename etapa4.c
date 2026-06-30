#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <mpi.h>

#define BUFFER_SIZE 5


typedef struct {
    int p[3];
} Clock;


typedef struct {
    int dest;
    Clock clock;
} SendItem;


int my_rank;
int running = 1; 
Clock local_clock = {{0,0,0}};


Clock fila_rec[BUFFER_SIZE];
int count_rec = 0, in_rec = 0, out_rec = 0;
pthread_mutex_t mutex_rec = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t naoCheia_rec = PTHREAD_COND_INITIALIZER;
pthread_cond_t naoVazia_rec = PTHREAD_COND_INITIALIZER;


SendItem fila_env[BUFFER_SIZE];
int count_env = 0, in_env = 0, out_env = 0;
pthread_mutex_t mutex_env = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t naoCheia_env = PTHREAD_COND_INITIALIZER;
pthread_cond_t naoVazia_env = PTHREAD_COND_INITIALIZER;


void PrintClock(const char *acao) {
    printf("[P%d] %s -> Clock = (%d,%d,%d)\n",
           my_rank, acao, local_clock.p[0], local_clock.p[1], local_clock.p[2]);
}


void enqueue_rec(Clock c) {
    pthread_mutex_lock(&mutex_rec);
    while (count_rec == BUFFER_SIZE) pthread_cond_wait(&naoCheia_rec, &mutex_rec);
    
    fila_rec[in_rec] = c;
    in_rec = (in_rec + 1) % BUFFER_SIZE;
    count_rec++;
    
    pthread_cond_signal(&naoVazia_rec);
    pthread_mutex_unlock(&mutex_rec);
}

Clock dequeue_rec() {
    pthread_mutex_lock(&mutex_rec);
    while (count_rec == 0) pthread_cond_wait(&naoVazia_rec, &mutex_rec);
    
    Clock c = fila_rec[out_rec];
    out_rec = (out_rec + 1) % BUFFER_SIZE;
    count_rec--;
    
    pthread_cond_signal(&naoCheia_rec);
    pthread_mutex_unlock(&mutex_rec);
    return c;
}

void enqueue_env(int dest, Clock c) {
    pthread_mutex_lock(&mutex_env);
    while (count_env == BUFFER_SIZE) pthread_cond_wait(&naoCheia_env, &mutex_env);
    
    fila_env[in_env].dest = dest;
    fila_env[in_env].clock = c;
    in_env = (in_env + 1) % BUFFER_SIZE;
    count_env++;
    
    pthread_cond_signal(&naoVazia_env);
    pthread_mutex_unlock(&mutex_env);
}

SendItem dequeue_env() {
    pthread_mutex_lock(&mutex_env);
    while (count_env == 0) pthread_cond_wait(&naoVazia_env, &mutex_env);
    
    SendItem item = fila_env[out_env];
    out_env = (out_env + 1) % BUFFER_SIZE;
    count_env--;
    
    pthread_cond_signal(&naoCheia_env);
    pthread_mutex_unlock(&mutex_env);
    return item;
}


void Event() {
    local_clock.p[my_rank]++;
    PrintClock("Evento interno");
    sleep(1); 
}

void Send(int dest) {
    local_clock.p[my_rank]++;
    enqueue_env(dest, local_clock); 
    PrintClock("Mensagem enviada p/ fila");
    sleep(1);
}

void Receive() {
    Clock msg = dequeue_rec(); 
    
    
    for (int i = 0; i < 3; i++) {
        if (local_clock.p[i] < msg.p[i]) {
            local_clock.p[i] = msg.p[i];
        }
    }
    local_clock.p[my_rank]++;
    PrintClock("Mensagem consumida da fila");
    sleep(1);
}



void* thread_receptora(void* arg) {
    
    while (running) {
        Clock msg;
        MPI_Status status;
        
        
        MPI_Recv(&msg.p, 3, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        if (msg.p[0] == -1) break; 
        
        enqueue_rec(msg);
    }
    return NULL;
}

void* thread_emissora(void* arg) {
    
    while (running) {
        SendItem item = dequeue_env();
        if (item.dest == -1) break; 
        
        MPI_Send(&item.clock.p, 3, MPI_INT, item.dest, 0, MPI_COMM_WORLD);
    }
    return NULL;
}

void* thread_central(void* arg) {
    PrintClock("Estado inicial");

    // Ordem dos eventos
    if (my_rank == 0) {
        Event();
        Send(1);
        Receive();
        Send(2);
        Receive();
        Send(1);
        Event();
    } 
    else if (my_rank == 1) {
        Send(0);
        Receive();
        Receive();
    } 
    else if (my_rank == 2) {
        Event();
        Send(0);
        Receive();
    }
    
    sleep(2);
    Clock terminate_msg = {{-1, -1, -1}};
    MPI_Send(&terminate_msg.p, 3, MPI_INT, my_rank, 0, MPI_COMM_WORLD); 
    enqueue_env(-1, terminate_msg); 

    return NULL;
}


int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    pthread_t receptora, emissora, central;

    
    pthread_create(&receptora, NULL, thread_receptora, NULL);
    pthread_create(&emissora, NULL, thread_emissora, NULL);
    pthread_create(&central, NULL, thread_central, NULL);

    
    pthread_join(central, NULL);
    running = 0; 
    
    pthread_join(receptora, NULL);
    pthread_join(emissora, NULL);

    MPI_Finalize();
    return 0;
}