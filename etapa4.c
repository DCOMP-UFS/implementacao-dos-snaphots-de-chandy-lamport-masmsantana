#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <mpi.h>

#define BUFFER_SIZE 5

/* Estrutura do relógio vetorial baseada na Etapa 1 */
typedef struct {
    int p[3];
} Clock;

/* Estrutura para os itens da Fila de Emissão */
typedef struct {
    int dest;
    Clock clock;
} SendItem;

/* NOVA: Estrutura para os itens da Fila de Recepção (Commit 1) */
typedef struct {
    int source;
    Clock clock;
} RecvItem;

int my_rank;
int running = 1; // Flag para desligamento limpo das threads
Clock local_clock = {{0,0,0}};

/* MODIFICADO: Fila de recepção agora usa RecvItem (Commit 1) */
RecvItem fila_rec[BUFFER_SIZE];
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

/* MODIFICADO: Função agora recebe um RecvItem (Commit 1) */
void enqueue_rec(RecvItem item) {
    pthread_mutex_lock(&mutex_rec);
    while (count_rec == BUFFER_SIZE) pthread_cond_wait(&naoCheia_rec, &mutex_rec);
    
    fila_rec[in_rec] = item;
    in_rec = (in_rec + 1) % BUFFER_SIZE;
    count_rec++;
    
    pthread_cond_signal(&naoVazia_rec);
    pthread_mutex_unlock(&mutex_rec);
}

/* MODIFICADO: Função agora retorna um RecvItem (Commit 1) */
RecvItem dequeue_rec() {
    pthread_mutex_lock(&mutex_rec);
    while (count_rec == 0) pthread_cond_wait(&naoVazia_rec, &mutex_rec);
    
    RecvItem item = fila_rec[out_rec];
    out_rec = (out_rec + 1) % BUFFER_SIZE;
    count_rec--;
    
    pthread_cond_signal(&naoCheia_rec);
    pthread_mutex_unlock(&mutex_rec);
    return item;
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

/* MODIFICADO: Adaptação provisória para lidar com o RecvItem (Commit 1) */
void Receive() {
    RecvItem item = dequeue_rec(); 
    Clock msg = item.clock; // Extrai o relógio do item recebido
    
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
        if (msg.p[0] == -1) break; // Terminação programada
        
        /* MODIFICADO: Cria o RecvItem usando MPI_SOURCE e repassa (Commit 1) */
        RecvItem item;
        item.source = status.MPI_SOURCE;
        item.clock = msg;
        
        enqueue_rec(item);
    }
    return NULL;
}

void* thread_emissora(void* arg) {
    while (running) {
        SendItem item = dequeue_env();
        if (item.dest == -1) break; // Terminação programada
        
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
    
    // Lógica para sinalizar o fim em todos os processos
    sleep(2);
    Clock terminate_msg = {{-1, -1, -1}};
    MPI_Send(&terminate_msg.p, 3, MPI_INT, my_rank, 0, MPI_COMM_WORLD); // Envia a si mesmo para soltar recepção
    enqueue_env(-1, terminate_msg); // Soltar emissora

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