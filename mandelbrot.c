// gcc -O3 mandelbrot.c -o mandelbrot -lpthread -lm
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <pthread.h>

#define X_RES 1024
#define Y_RES 768
#define MAX_ITER 200
#define NUM_REPLICACOES 10
#define CSV_FILENAME "mandelbrot_c_pthreads.csv"
#define MONITOR_INTERVAL_MS 1

// Suporte a API do Windows ou POSIX/Linux
#ifdef _WIN32
    #include <windows.h>
    #include <psapi.h>

    static double get_time_in_seconds(void) {
        LARGE_INTEGER frequency, counter;
        QueryPerformanceFrequency(&frequency);
        QueryPerformanceCounter(&counter);
        return (double)counter.QuadPart / (double)frequency.QuadPart;
    }

    // Retorna uso de RAM em MB do processo atual
    static double get_ram_usage_mb(void) {
        PROCESS_MEMORY_COUNTERS pmc;
        if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
            return (double)pmc.WorkingSetSize / (1024.0 * 1024.0);
        }
        return 0.0;
    }

    // Retorna tempo de CPU acumulado (Kernel + User) em segundos
    static double get_process_cpu_time(void) {
        FILETIME ftCreation, ftExit, ftKernel, ftUser;
        if (GetProcessTimes(GetCurrentProcess(), &ftCreation, &ftExit, &ftKernel, &ftUser)) {
            ULARGE_INTEGER k, u;
            k.LowPart = ftKernel.dwLowDateTime;
            k.HighPart = ftKernel.dwHighDateTime;
            u.LowPart = ftUser.dwLowDateTime;
            u.HighPart = ftUser.dwHighDateTime;
            return (double)(k.QuadPart + u.QuadPart) / 10000000.0; // Converte de 100ns para segundos
        }
        return 0.0;
    }

    static void sleep_ms(int ms) {
        Sleep(ms);
    }
#else
    #define _POSIX_C_SOURCE 199309L
    #include <time.h>
    #include <unistd.h>
    #include <sys/resource.h>

    static double get_time_in_seconds(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec + ts.tv_nsec / 1e9;
    }

    static double get_ram_usage_mb(void) {
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            #ifdef __APPLE__
                return (double)usage.ru_maxrss / (1024.0 * 1024.0);
            #else
                return (double)usage.ru_maxrss / 1024.0; // Linux ru_maxrss é em KB
            #endif
        }
        return 0.0;
    }

    static double get_process_cpu_time(void) {
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            double user = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6;
            double sys = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
            return user + sys;
        }
        return 0.0;
    }

    static void sleep_ms(int ms) {
        usleep(ms * 1000);
    }
#endif

// Estruturas do Mandelbrot
typedef struct {
    unsigned char r, g, b;
} RGBColor;

typedef struct {
    int left_width, right_width;
    int left_height, right_height;
} BlockCoords;

typedef struct {
    BlockCoords block;
    int max_iter, x_res, y_res;
    RGBColor **output_matrix;
} ThreadData;

// Estrutura para controle da Thread de Monitoramento
typedef struct {
    volatile int keep_running;
    double sum_cpu_pct;
    double sum_ram_mb;
    int sample_count;
} MonitorArgs;

// Thread de amostragem de CPU e RAM
void* resource_monitor_thread(void *arg) {
    MonitorArgs *m = (MonitorArgs*) arg;
    
    double last_cpu_time = get_process_cpu_time();
    double last_time = get_time_in_seconds();

    while (m->keep_running) {
        sleep_ms(MONITOR_INTERVAL_MS);

        double now = get_time_in_seconds();
        double dt = now - last_time;
        if (dt <= 0.0001) dt = 0.0001;

        double curr_cpu_time = get_process_cpu_time();
        double cpu_dt = curr_cpu_time - last_cpu_time;
        if (cpu_dt < 0) cpu_dt = 0;

        // Porcentagem acumulada de CPU (1 núcleo 100% ocupado = 100.0%)
        double cpu_pct = (cpu_dt / dt) * 100.0;
        double ram_mb = get_ram_usage_mb();

        m->sum_cpu_pct += cpu_pct;
        m->sum_ram_mb += ram_mb;
        m->sample_count++;

        last_cpu_time = curr_cpu_time;
        last_time = now;
    }

    pthread_exit(NULL);
}

RGBColor get_heat_color(int iter_count, int max_iter) {
    RGBColor color;
    if (iter_count == max_iter) {
        color.r = color.g = color.b = 0;
        return color;
    }
    double t = 1.0 - (log(iter_count + 1.0) / log(max_iter + 1.0));
    color.r = (unsigned char)((sin(t * 3.0 + 0.0) * 0.5 + 0.5) * 255.0);
    color.g = (unsigned char)((sin(t * 3.0 + 1.5) * 0.5 + 0.5) * 255.0);
    color.b = (unsigned char)((sin(t * 3.0 + 3.0) * 0.5 + 0.5) * 255.0);
    return color;
}

void* worker_task(void* arg) {
    ThreadData* data = (ThreadData*) arg;
    BlockCoords block = data->block;

    for (int a = block.left_width; a <= block.right_width; a++) {
        for (int b = block.left_height; b <= block.right_height; b++) {
            double ca = (a - data->x_res / 2.0) * 4.0 / data->x_res;
            double cb = (b - data->y_res / 2.0) * 4.0 / data->y_res;
            double x = 0.0, y = 0.0;
            int iter_count = 0;

            while (iter_count < data->max_iter) {
                double x_old = x;
                x = x * x - y * y + ca;
                y = 2.0 * x_old * y + cb;
                if ((x * x + y * y) > 4.0) break;
                iter_count++;
            }

            int local_a = a - block.left_width;
            int local_b = b - block.left_height;
            data->output_matrix[local_a][local_b] = get_heat_color(iter_count, data->max_iter);
        }
    }
    pthread_exit(NULL);
}

BlockCoords* divide_blocks(int num_blocks_side, int x_res, int y_res, int *total_blocks) {
    *total_blocks = num_blocks_side * num_blocks_side;
    BlockCoords *blocks = (BlockCoords*) malloc((*total_blocks) * sizeof(BlockCoords));

    int idx = 0;
    for (int i = 0; i < num_blocks_side; i++) {
        int x_start = (i * x_res) / num_blocks_side;
        int x_end = (i < num_blocks_side - 1) ? (((i + 1) * x_res) / num_blocks_side) : x_res;

        for (int j = 0; j < num_blocks_side; j++) {
            int y_start = (j * y_res) / num_blocks_side;
            int y_end = (j < num_blocks_side - 1) ? (((j + 1) * y_res) / num_blocks_side) : y_res;

            blocks[idx].left_width = x_start;
            blocks[idx].right_width = x_end - 1;
            blocks[idx].left_height = y_start;
            blocks[idx].right_height = y_end - 1;
            idx++;
        }
    }
    return blocks;
}

// Executa o teste coletando tempo, CPU e RAM
void run_single_mandelbrot_worker(int num_workers, int num_blocks_side, double *out_time, double *out_cpu_avg, double *out_ram_mb_avg) {
    // Inicializa e lança a thread de monitoramento
    pthread_t monitor_thread;
    MonitorArgs mon_args = { .keep_running = 1, .sum_cpu_pct = 0.0, .sum_ram_mb = 0.0, .sample_count = 0 };
    pthread_create(&monitor_thread, NULL, resource_monitor_thread, &mon_args);

    double start_time = get_time_in_seconds();

    int total_blocks = 0;
    BlockCoords *blocks = divide_blocks(num_blocks_side, X_RES, Y_RES, &total_blocks);

    pthread_t threads[total_blocks];
    ThreadData thread_data[total_blocks];

    for (int i = 0; i < total_blocks; i++) {
        int width = blocks[i].right_width - blocks[i].left_width + 1;
        RGBColor **sub_matrix = (RGBColor**) malloc(width * sizeof(RGBColor*));
        for (int w = 0; w < width; w++) {
            int height = blocks[i].right_height - blocks[i].left_height + 1;
            sub_matrix[w] = (RGBColor*) malloc(height * sizeof(RGBColor));
        }

        thread_data[i].block = blocks[i];
        thread_data[i].max_iter = MAX_ITER;
        thread_data[i].x_res = X_RES;
        thread_data[i].y_res = Y_RES;
        thread_data[i].output_matrix = sub_matrix;

        pthread_create(&threads[i], NULL, worker_task, (void*)&thread_data[i]);
    }

    for (int i = 0; i < total_blocks; i++) {
        pthread_join(threads[i], NULL);
        int width = blocks[i].right_width - blocks[i].left_width + 1;
        for (int w = 0; w < width; w++) {
            free(thread_data[i].output_matrix[w]);
        }
        free(thread_data[i].output_matrix);
    }
    free(blocks);

    double end_time = get_time_in_seconds();

    // Interrompe o monitoramento
    mon_args.keep_running = 0;
    pthread_join(monitor_thread, NULL);

    *out_time = end_time - start_time;
    *out_cpu_avg = (mon_args.sample_count > 0) ? (mon_args.sum_cpu_pct / mon_args.sample_count) : 0.0;
    *out_ram_mb_avg = (mon_args.sample_count > 0) ? (mon_args.sum_ram_mb / mon_args.sample_count) : get_ram_usage_mb();
}

void run_benchmark_orchestrator() {
    FILE *f = fopen(CSV_FILENAME, "w");
    if (!f) {
        perror("Erro ao criar arquivo CSV");
        return;
    }

    // Cabeçalho idêntico ao gerado pelos benchmarks em Python
    fprintf(f, "implementacao,mecanismo,workers,replicacao,tempo_execucao_s,cpu_media_porcentagem,ram_media_porcentagem,ram_mb\n");
    fclose(f);

    int worker_counts[] = {1, 2, 4, 8};
    int num_scenarios = sizeof(worker_counts) / sizeof(worker_counts[0]);

    for (int i = 0; i < num_scenarios; i++) {
        int workers = worker_counts[i];
        int blocks_side = workers;

        printf("\n============================================================\n");
        printf("[%d/%d] Executando Cenario: C-pthreads-w%d\n", i + 1, num_scenarios, workers);
        printf("============================================================\n");

        for (int rep = 1; rep <= NUM_REPLICACOES; rep++) {
            double tempo = 0.0, cpu_pct = 0.0, ram_mb = 0.0;
            run_single_mandelbrot_worker(workers, blocks_side, &tempo, &cpu_pct, &ram_mb);

            // Suposição básica de % de RAM em relação à memória do sistema
            double ram_pct = 0.0; 

            f = fopen(CSV_FILENAME, "a");
            if (f) {
                fprintf(f, "c_pthreads,pthread,%d,%d,%.6f,%.2f,%.2f,%.2f\n", 
                        workers, rep, tempo, cpu_pct, ram_pct, ram_mb);
                fclose(f);
            }

            printf("  Replica %d/%d | Tempo: %.4fs | CPU Média: %.2f%% | RAM (MB): %.2f\n", 
                   rep, NUM_REPLICACOES, tempo, cpu_pct, ram_mb);
        }
    }
}

int main() {
    run_benchmark_orchestrator();
    return 0;
}