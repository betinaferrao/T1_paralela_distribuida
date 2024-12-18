#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define TRUE 1
#define FALSE 0
#define MAX_TASKS 10  


typedef struct {
    int id_conta;
    double saldo;
    pthread_mutex_t conta_mutex;
} Conta;

typedef struct {
    Conta *contas;
    int quantidade;
    int taxa_requisicao;
} Banco;

typedef struct {
    int num_op_concluidas;
    int pronto; 
    pthread_mutex_t balanco_mutex;
    pthread_cond_t balanco_cond;
} var_cond_balanco;

typedef struct {
    int valor;
    int conta_id;
    Banco *banco;
    var_cond_balanco *cond_balanco;
} SaqueDeposito;

typedef struct {
    int valor;
    int conta_origem;
    int conta_destino;
    Banco *banco;
    var_cond_balanco *cond_balanco;
} Transferencia;

typedef enum { LIVRE, OCUPADA } thread_status_t;

typedef struct task {
    void (*function)(void *);
    void *arg;
    struct task *next;
} task_t;

typedef struct {
    pthread_t thread;
    int thread_id;
    thread_status_t status;
} worker_thread_t;

typedef struct {
    int num_workers;
    worker_thread_t *workers;
    task_t *internal_tasks;  
    int task_count;
    pthread_mutex_t lock;   
    pthread_cond_t cond;   
} thread_pool_t;

int num_accounts;
int taxa_requisicao; 

// Variáveis da fila principal
task_t *task_queue = NULL;
task_t *queue_tail = NULL;
int queue_size = 0;

// Mutex e semáforos para sincronização na fila principal
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
sem_t cheio;  
sem_t vazio; 


// INICIALIZAÇÕES

// Inicializa o banco com contas e mutexes
void init_banco(Banco *banco, int num_accounts, int taxa_requisicao) {
    banco->contas = malloc(num_accounts * sizeof(Conta));
    banco->quantidade = num_accounts;
    banco->taxa_requisicao = taxa_requisicao;
    for (int i = 0; i < num_accounts; i++) {
        banco->contas[i].id_conta = i;
        banco->contas[i].saldo = (rand() % 9901) + 100.0;
        pthread_mutex_init(&banco->contas[i].conta_mutex, NULL);
        printf("Conta %d criada com saldo inicial de %.2f\n", banco->contas[i].id_conta, banco->contas[i].saldo);
    }
}

// Inicializa semáforos e mutexes
void init_sync() {
    sem_init(&cheio, 0, 0);
    sem_init(&vazio, 0, MAX_TASKS);
}

// Inicializa a variável de condição do balanco geral
void init_cond_balanco(var_cond_balanco *estado) {
    estado->pronto = FALSE;
    estado->num_op_concluidas = 0;
    pthread_mutex_init(&estado->balanco_mutex, NULL);
    pthread_cond_init(&estado->balanco_cond, NULL);
}


// FUNÇÕES

// Função de saque ou depósito
void realizar_saque_deposito(void *arg) {
    SaqueDeposito *operacao = (SaqueDeposito *)arg;         
    Conta *conta = &operacao->banco->contas[operacao->conta_id];
    pthread_mutex_lock(&conta->conta_mutex);
    
    double novo_saldo = conta->saldo + operacao->valor;
    conta->saldo = novo_saldo;
    printf("%s de %d na conta %d, Saldo atualizado: %.2f\n", 
            operacao->valor >= 0 ? "[DEPÓSITO]" : "[SAQUE]", operacao->valor, operacao->conta_id, conta->saldo);

    sleep(4);
    pthread_mutex_unlock(&conta->conta_mutex);

    pthread_mutex_lock(&operacao->cond_balanco->balanco_mutex);
    operacao->cond_balanco->num_op_concluidas++;
    if(operacao->cond_balanco->num_op_concluidas%10==0){
        operacao->cond_balanco->pronto = TRUE; 
        pthread_cond_signal(&operacao->cond_balanco->balanco_cond); 
    }
    pthread_mutex_unlock(&operacao->cond_balanco->balanco_mutex);
    free(operacao);
}

// Função de transferência
void realizar_transferencia(void *arg) {
    Transferencia *operacao = (Transferencia *)arg;           
    Conta *conta_origem = &operacao->banco->contas[operacao->conta_origem];
    Conta *conta_destino = &operacao->banco->contas[operacao->conta_destino];

    Conta *first_lock = conta_origem->id_conta < conta_destino->id_conta ? conta_origem : conta_destino;
    Conta *second_lock = conta_origem->id_conta < conta_destino->id_conta ? conta_destino : conta_origem;

    pthread_mutex_lock(&first_lock->conta_mutex);
    pthread_mutex_lock(&second_lock->conta_mutex);


    conta_origem->saldo -= operacao->valor;
    conta_destino->saldo += operacao->valor;
    printf("[TRANSFERÊNCIA] de %d da conta %d para a conta %d. Novo saldo origem: %.2f, destino: %.2f\n",
            operacao->valor, operacao->conta_origem, operacao->conta_destino, conta_origem->saldo, conta_destino->saldo);
    
    sleep(4);
    pthread_mutex_unlock(&second_lock->conta_mutex);
    pthread_mutex_unlock(&first_lock->conta_mutex);

    pthread_mutex_lock(&operacao->cond_balanco->balanco_mutex);
    operacao->cond_balanco->num_op_concluidas++;
    if(operacao->cond_balanco->num_op_concluidas%10==0){
        operacao->cond_balanco->pronto = TRUE;
        pthread_cond_signal(&operacao->cond_balanco->balanco_cond);
    }
    pthread_mutex_unlock(&operacao->cond_balanco->balanco_mutex);
    free(operacao);
}

// Função que exibe o valor de cada uma das contas
void balanco_geral(void *balanco_args) {
    Banco *banco = (Banco *)((void **)balanco_args)[0];
    var_cond_balanco *cond_balanco = (var_cond_balanco *)((void **)balanco_args)[1];

    printf("[BALANÇO] das Contas:\n");
    for (int i = 0; i < banco->quantidade; i++) {
        printf("Conta ID: %d, Saldo: %.2f\n", banco->contas[i].id_conta, banco->contas[i].saldo);
    }


    cond_balanco->num_op_concluidas = 0;
    cond_balanco->pronto = FALSE;
    pthread_mutex_unlock(&cond_balanco->balanco_mutex);
    free(balanco_args);
}


// FILA

// Adiciona tarefa a fila
void enqueue_task(void (*function)(void *), void *arg) {
    sem_wait(&vazio);  
    pthread_mutex_lock(&queue_mutex);

    task_t *new_task = malloc(sizeof(task_t));
    new_task->function = function;
    new_task->arg = arg;
    new_task->next = NULL;

    if (task_queue == NULL) {
        task_queue = new_task;
        queue_tail = new_task;
    } else {
        queue_tail->next = new_task;
        queue_tail = new_task;
    }
    queue_size++;

    printf("[ENQUEUE] Tarefa adicionada. Tamanho da fila: %d\n", queue_size);

    pthread_mutex_unlock(&queue_mutex);  
    sem_post(&cheio); 
}

// Desenfileira uma tarefa da fila principal
task_t *dequeue_task() {
    sem_wait(&cheio); 
    pthread_mutex_lock(&queue_mutex);

    task_t *task = task_queue;
    task_queue = task_queue->next;
    if (task_queue == NULL) {
        queue_tail = NULL;
    }
    queue_size--;

    printf("[DEQUEUE] Tarefa removida. Tamanho da fila: %d\n", queue_size);

    pthread_mutex_unlock(&queue_mutex); 
    sem_post(&vazio);

    return task;
}


// Thread produtora que gera as operações e adiciona na fila
void *client_thread(void *arg) {
    void *arguments = (void *)arg;
    int *num_sleep = (int *)((void **)arguments)[0];
    Banco *banco = (Banco *)((void **)arguments)[1];
    var_cond_balanco *cond_balanco = (var_cond_balanco *)((void **)arguments)[2];
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned long)pthread_self();


    while (1) {
        int operacao_tipo;
        int rand_val = rand_r(&seed) % 100;

        if (rand_val < banco->taxa_requisicao) {
            operacao_tipo = 0; 
        } else {
            operacao_tipo = 1;
        }
        printf("Valor randômico: %d, taxa: %d\n", rand_val, banco->taxa_requisicao);
        
        int contaA = rand_r(&seed) % num_accounts;
        int valor;

        if (operacao_tipo == 0) {
            valor = (rand_r(&seed) % 2000) - 1000;
            SaqueDeposito *dados_operacao = malloc(sizeof(SaqueDeposito));
            dados_operacao->valor = valor;
            dados_operacao->conta_id = contaA;
            dados_operacao->banco = banco;
            dados_operacao->cond_balanco = cond_balanco;
            enqueue_task(realizar_saque_deposito, (void *)dados_operacao);
        } else {
            int contaB;
            do {
                contaB = rand_r(&seed) % num_accounts;
            } while (contaA == contaB);

            valor = rand_r(&seed) % 1000;
            Transferencia *dados_operacao = malloc(sizeof(Transferencia));
            dados_operacao->valor = valor;
            dados_operacao->conta_origem = contaA;
            dados_operacao->conta_destino = contaB;
            dados_operacao->banco = banco;
            dados_operacao->cond_balanco = cond_balanco;
            enqueue_task(realizar_transferencia, (void *)dados_operacao);
        }

        sleep(*num_sleep);
    }
}


// Executa uma tarefa no pool de threads
void submit_thread_pool(thread_pool_t *pool, task_t *task) {
    pthread_mutex_lock(&pool->lock);

    if (pool->internal_tasks == NULL) {
        pool->internal_tasks = task; 
    } else {
        task_t *current = pool->internal_tasks;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = task;
    }
    task->next = NULL; 

    printf("[SERVIDOR] Tarefa adicionada à lista interna do pool.\n");

    pool->task_count++;

    printf("[POOL] %d tarefa(s) no pool.\n", pool->task_count);

    pthread_cond_signal(&pool->cond); 
    pthread_mutex_unlock(&pool->lock);
}

// Thread servidor que pega tarefas da fila principal e passa para o pool
void *server_thread(void *arg) {
    void *args = (void *)arg;  
    thread_pool_t *pool = (thread_pool_t *)((void **)args)[0];
    Banco *banco = (Banco *)((void **)args)[1];
    var_cond_balanco *cond_balanco = (var_cond_balanco *)((void **)args)[2];

    for(int i=0; i<11; i++) {
        if(i < 10) {
            task_t *task = dequeue_task();
            pthread_mutex_lock(&cond_balanco->balanco_mutex);
            submit_thread_pool(pool, task);
            pthread_mutex_unlock(&cond_balanco->balanco_mutex);

        } else {
            pthread_mutex_lock(&cond_balanco->balanco_mutex);
            while(!cond_balanco->pronto){
                pthread_cond_wait(&cond_balanco->balanco_cond, &cond_balanco->balanco_mutex);
            }
            task_t *tarefa_balanco = malloc(sizeof(task_t));
            tarefa_balanco->function = balanco_geral;
            void **balanco_args = malloc(2 * sizeof(void *));
            balanco_args[0] = banco;
            balanco_args[1] = cond_balanco;
            tarefa_balanco->arg = balanco_args;
            submit_thread_pool(pool, tarefa_balanco);

            i = -1; 
        }

    }

}


// Função trabalhadora que executa tarefas do pool
void *worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t *)arg;

    int thread_id = -1;
    for (int i = 0; i < pool->num_workers; ++i) {
        if (pthread_equal(pthread_self(), pool->workers[i].thread)) {
            thread_id = pool->workers[i].thread_id;
            break;
        }
    }

    while (1) {
        pthread_mutex_lock(&pool->lock);

        while (pool->internal_tasks == NULL) {
            pthread_cond_wait(&pool->cond, &pool->lock);
        }

        task_t *task = pool->internal_tasks;
        pool->internal_tasks = task->next;
        pool->task_count--;

        pool->workers[thread_id].status = OCUPADA;
        printf("[TRABALHADORA] Thread %d executando tarefa...\n", thread_id);

        pthread_mutex_unlock(&pool->lock);

        task->function(task->arg);

        free(task);

        pthread_mutex_lock(&pool->lock);
        pool->workers[thread_id].status = LIVRE;
        printf("[TRABALHADORA] Thread %d está LIVRE.\n", thread_id);
        pthread_mutex_unlock(&pool->lock);
    }
}


// Inicializa o pool de threads
void thread_pool_init(thread_pool_t *pool, int num_workers) {
    pool->num_workers = num_workers;
    pool->workers = malloc(sizeof(worker_thread_t) * num_workers);
    pool->internal_tasks = NULL;
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->cond, NULL);

    for (int i = 0; i < num_workers; ++i) {
        pool->workers[i].thread_id = i;
        pool->workers[i].status = LIVRE;
        pthread_create(&pool->workers[i].thread, NULL, worker_thread, pool);
    }
}

// Destroi semáforos e mutexes
void destroy_sync() {
    sem_destroy(&cheio);
    sem_destroy(&vazio);
    pthread_mutex_destroy(&queue_mutex);
}

void thread_pool_destroy(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->lock);

    for (int i = 0; i < pool->num_workers; ++i) {
        printf("[POOL] Finalizando thread %d...\n", pool->workers[i].thread_id);
    }
    
    pthread_cond_broadcast(&pool->cond); 
    pthread_mutex_unlock(&pool->lock);

    for (int i = 0; i < pool->num_workers; ++i) {
        pthread_join(pool->workers[i].thread, NULL);
        printf("[POOL] Thread %d finalizada.\n", pool->workers[i].thread_id);
    }

    pthread_mutex_lock(&pool->lock);
    task_t *current_task = pool->internal_tasks;
    while (current_task != NULL) {
        task_t *next_task = current_task->next;
        free(current_task); 
        current_task = next_task;
    }
    pthread_mutex_unlock(&pool->lock);

    free(pool->workers);
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->cond);

    printf("[POOL] Pool de threads destruído com sucesso.\n");
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        fprintf(stderr, "É preciso inserir: %s <num_workers> <num_clientes> <num_contas> <num_sleep> <taxa_requisicao>\n", argv[0]);
        return 1;
    }

    int num_workers = atoi(argv[1]);
    int num_clients = atoi(argv[2]);
    num_accounts = atoi(argv[3]);
    int num_sleep = atoi(argv[4]);
    int taxa_requisicao = atoi(argv[5]);

    Banco banco;
    init_banco(&banco, num_accounts, taxa_requisicao);

    var_cond_balanco cond_balanco;
    init_cond_balanco(&cond_balanco);
    void **arguments = malloc(3 * sizeof(void *));
    arguments[0] = (void *)&num_sleep;  
    arguments[1] = (void *)&banco;
    arguments[2] = (void *)&cond_balanco;

    pthread_t id_threads[num_clients];
    for (int i = 0; i < num_clients; i++) {
        if (pthread_create(&id_threads[i], NULL, client_thread, (void *)arguments) != 0) {
            fprintf(stderr, "Erro ao criar thread %u\n", i);
            exit(EXIT_FAILURE);
        }
    }

    init_sync();

    thread_pool_t pool;
    thread_pool_init(&pool, num_workers);

    void **args = malloc(3 * sizeof(void *));
    args[0] = (void *)&pool;  
    args[1] = (void *)&banco;
    args[2] = (void *)&cond_balanco;
    pthread_t server;
    pthread_create(&server, NULL, server_thread, (void *)args);

    for (int i = 0; i < num_clients; ++i) {
        pthread_join(id_threads[i], NULL);
    }

    pthread_join(server, NULL);
    thread_pool_destroy(&pool);
    destroy_sync();

    return 0;
}
