// gcc -O3 -pthread c_fft_benchmark.c  -o c_fft_benchmark -lm
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <complex.h>
#include <pthread.h>

#define TOTAL_SIGNALS 16384
#define SIGNAL_SIZE 1024
#define NUM_REPLICACOES 10
#define CSV_FILENAME "fft_c_pthreads.csv"
#define MONITOR_INTERVAL_MS 20

#ifdef _WIN32
    #include <windows.h>
    #include <psapi.h>

    static double get_time_in_seconds(void) {
        LARGE_INTEGER frequency, counter;
        QueryPerformanceFrequency(&frequency);
        QueryPerformanceCounter(&counter);
        return (double)counter.QuadPart / (double)frequency.QuadPart;
    }

    static double get_ram_usage_mb(void) {
        PROCESS_MEMORY_COUNTERS pmc;
        if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
            return (double)pmc.WorkingSetSize / (1024.0 * 1024.0);
        }
        return 0.0;
    }

    static double get_process_cpu_time(void) {
        FILETIME ftCreation, ftExit, ftKernel, ftUser;
        if (GetProcessTimes(GetCurrentProcess(), &ftCreation, &ftExit, &ftKernel, &ftUser)) {
            ULARGE_INTEGER k, u;
            k.LowPart = ftKernel.dwLowDateTime;
            k.HighPart = ftKernel.dwHighDateTime;
            u.LowPart = ftUser.dwLowDateTime;
            u.HighPart = ftUser.dwHighDateTime;
            return (double)(k.QuadPart + u.QuadPart) / 10000000.0;
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
                return (double)usage.ru_maxrss / 1024.0; // Linux ru_maxrss em KB
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

// Estrutura para controle da Thread de Monitoramento
typedef struct {
    volatile int keep_running;
    double sum_cpu_pct;
    double sum_ram_mb;
    int sample_count;
} MonitorArgs;

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

// Implementação iterativa da FFT (Cooley-Tukey)
void fft_iterative(double complex *x, int n) {
    if (n <= 1) return;

    int j = 0;
    for (int i = 0; i < n - 1; i++) {
        if (i < j) {
            double complex temp = x[i];
            x[i] = x[j];
            x[j] = temp;
        }
        int bit = n >> 1;
        while (j & bit) {
            j ^= bit;
            bit >>= 1;
        }
        j ^= bit;
    }

    for (int length = 2; length <= n; length <<= 1) {
        int half_length = length / 2;
        double angle = -2.0 * M_PI / length;
        double complex w_step = cexp(I * angle);

        for (int i = 0; i < n; i += length) {
            double complex w = 1.0 + 0.0 * I;
            for (int k = 0; k < half_length; k++) {
                double complex u = x[i + k];
                double complex v = x[i + k + half_length] * w;
                x[i + k] = u + v;
                x[i + k + half_length] = u - v;
                w *= w_step;
            }
        }
    }
}

// Estrutura de dados para o worker da FFT
typedef struct {
    double complex *signals;
    int start_idx;
    int end_idx;
    int signal_size;
} WorkerTaskArgs;

void* worker_task(void *arg) {
    WorkerTaskArgs *args = (WorkerTaskArgs *)arg;
    for (int idx = args->start_idx; idx < args->end_idx; idx++) {
        double complex *sig = args->signals + (idx * args->signal_size);
        fft_iterative(sig, args->signal_size);
    }
    pthread_exit(NULL);
}

// Geração determinística de sinais (equivalente ao random.Random(42))
void generate_signals(double complex *signals, int total_signals, int signal_size) {
    unsigned int seed = 42;
    for (int i = 0; i < total_signals * signal_size; i++) {
        seed = seed * 1103515245 + 12345;
        double real = (double)(seed % 10000) / 10000.0;
        seed = seed * 1103515245 + 12345;
        double imag = (double)(seed % 10000) / 10000.0;
        signals[i] = real + imag * I;
    }
}

// Executa uma réplica do benchmark dividindo o lote entre N workers
void run_single_fft_worker(int num_workers, double complex *all_signals, double *out_time, double *out_cpu_avg, double *out_ram_mb_avg) {
    // Recria os sinais deterministicamente para cada réplica
    generate_signals(all_signals, TOTAL_SIGNALS, SIGNAL_SIZE);

    pthread_t monitor_thread;
    MonitorArgs mon_args = { .keep_running = 1, .sum_cpu_pct = 0.0, .sum_ram_mb = 0.0, .sample_count = 0 };
    pthread_create(&monitor_thread, NULL, resource_monitor_thread, &mon_args);

    double start_time = get_time_in_seconds();

    pthread_t *threads = (pthread_t *)malloc(num_workers * sizeof(pthread_t));
    WorkerTaskArgs *thread_args = (WorkerTaskArgs *)malloc(num_workers * sizeof(WorkerTaskArgs));

    int chunk_size = (TOTAL_SIGNALS + num_workers - 1) / num_workers;

    for (int i = 0; i < num_workers; i++) {
        int start = i * chunk_size;
        int end = start + chunk_size;
        if (end > TOTAL_SIGNALS) end = TOTAL_SIGNALS;

        thread_args[i].signals = all_signals;
        thread_args[i].start_idx = start;
        thread_args[i].end_idx = end;
        thread_args[i].signal_size = SIGNAL_SIZE;

        if (start < TOTAL_SIGNALS) {
            pthread_create(&threads[i], NULL, worker_task, &thread_args[i]);
        } else {
            threads[i] = 0;
        }
    }

    for (int i = 0; i < num_workers; i++) {
        if (thread_args[i].start_idx < TOTAL_SIGNALS) {
            pthread_join(threads[i], NULL);
        }
    }

    double end_time = get_time_in_seconds();

    mon_args.keep_running = 0;
    pthread_join(monitor_thread, NULL);

    *out_time = end_time - start_time;
    *out_cpu_avg = (mon_args.sample_count > 0) ? (mon_args.sum_cpu_pct / mon_args.sample_count) : 0.0;
    *out_ram_mb_avg = (mon_args.sample_count > 0) ? (mon_args.sum_ram_mb / mon_args.sample_count) : get_ram_usage_mb();

    free(threads);
    free(thread_args);
}

void run_benchmark_orchestrator() {
    FILE *f = fopen(CSV_FILENAME, "w");
    if (!f) {
        perror("Erro ao criar arquivo CSV");
        return;
    }

    fprintf(f, "implementacao,mecanismo,workers,replicacao,tempo_execucao_s,cpu_media_porcentagem,ram_media_porcentagem,ram_mb\n");
    fclose(f);

    int worker_counts[] = {1, 2, 4, 8};
    int num_scenarios = sizeof(worker_counts) / sizeof(worker_counts[0]);

    // Aloca os sinais globalmente para o benchmark
    double complex *all_signals = (double complex *)malloc(TOTAL_SIGNALS * SIGNAL_SIZE * sizeof(double complex));
    if (!all_signals) {
        perror("Erro de alocação de memória");
        return;
    }

    for (int i = 0; i < num_scenarios; i++) {
        int workers = worker_counts[i];

        printf("\n============================================================\n");
        printf("[%d/%d] Executando Cenário FFT: C-pthreads-w%d\n", i + 1, num_scenarios, workers);
        printf("============================================================\n");

        for (int rep = 1; rep <= NUM_REPLICACOES; rep++) {
            double tempo = 0.0, cpu_pct = 0.0, ram_mb = 0.0;
            run_single_fft_worker(workers, all_signals, &tempo, &cpu_pct, &ram_mb);

            double ram_pct = 0.0; // Opcional ou calculado se necessário

            f = fopen(CSV_FILENAME, "a");
            if (f) {
                fprintf(f, "c_pthreads,thread,%d,%d,%.6f,%.2f,%.2f,%.2f\n", 
                        workers, rep, tempo, cpu_pct, ram_pct, ram_mb);
                fclose(f);
            }

            printf("  Réplica %d/%d | Tempo: %.4fs | CPU Média: %.2f%% | RAM (MB): %.2f\n", 
                   rep, NUM_REPLICACOES, tempo, cpu_pct, ram_mb);
        }
    }

    free(all_signals);
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("=== INICIALIZANDO PROGRAMA ===\n");
    fflush(stdout);

    run_benchmark_orchestrator();

    printf("=== FIM DO PROGRAMA ===\n");
    fflush(stdout);
    return 0;
}