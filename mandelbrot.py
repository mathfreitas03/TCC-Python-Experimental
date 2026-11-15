from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from time import time, sleep
from itertools import repeat
import subprocess
import itertools
import argparse
import math
import psutil
import threading
import csv
import os
import multiprocessing as mp

NUM_CORES = os.cpu_count() or 8
ENV_TRADITIONAL = os.path.join("venv_traditional", "Scripts", "python.exe")
ENV_FREETHREADED = os.path.join("venv_free_threaded", "Scripts", "python.exe")
NUM_REPLICACOES = 10

from resource_monitor import ExternalResourceMonitor

# Classes prototipo

# class ExternalResourceMonitor(mp.Process):
#     def __init__(self, target_pid: int, interval: float = 0.05):
#         super().__init__()
#         self.target_pid = target_pid
#         self.interval = interval
#         self.stop_event = mp.Event()
#         self.cpu_samples = mp.Queue()
#         self.ram_samples = mp.Queue()

#     def run(self):
#         monitor_pid = os.getpid() # Descobre o PID do PRÓPRIO monitor para ignorar
        
#         try:
#             target_proc = psutil.Process(self.target_pid)
#         except (psutil.NoSuchProcess, psutil.AccessDenied):
#             return

#         # Dicionários para manter o tempo acumulado de CPU por PID: {pid: tempo_cpu}
#         last_cpus = {}

#         # Primeira leitura de calibração
#         try:
#             p_times = target_proc.cpu_times()
#             last_cpus[self.target_pid] = p_times.user + p_times.system
#         except Exception:
#             pass

#         last_time = time()

#         while not self.stop_event.is_set():
#             sleep(self.interval)
            
#             try:
#                 now = time()
#                 dt = max(0.0001, now - last_time)

#                 current_cpus = {}
#                 cpu_time_total = 0.0
#                 ram_bytes_total = 0

#                 # 1. Mede o processo principal do benchmark
#                 if not target_proc.is_running():
#                     break

#                 p_times = target_proc.cpu_times()
#                 curr_cpu = p_times.user + p_times.system
#                 current_cpus[self.target_pid] = curr_cpu
                
#                 prev_cpu = last_cpus.get(self.target_pid, curr_cpu)
#                 cpu_time_total += max(0.0, curr_cpu - prev_cpu)
#                 ram_bytes_total += target_proc.memory_info().rss

#                 # 2. Mede os processos filhos (caso use ProcessPoolExecutor)
#                 for child in target_proc.children(recursive=True):
#                     try:
#                         # IGNORA o próprio processo de monitoramento
#                         if child.pid == monitor_pid:
#                             continue

#                         if child.is_running():
#                             c_pid = child.pid
#                             c_times = child.cpu_times()
#                             c_curr_cpu = c_times.user + c_times.system
#                             current_cpus[c_pid] = c_curr_cpu

#                             c_prev_cpu = last_cpus.get(c_pid, c_curr_cpu)
#                             cpu_time_total += max(0.0, c_curr_cpu - c_prev_cpu)
#                             ram_bytes_total += child.memory_info().rss
#                     except (psutil.NoSuchProcess, psutil.AccessDenied):
#                         continue

#                 # 3. Calcula a porcentagem real (1 núcleo cheio = 100.0%)
#                 cpu_pct = (cpu_time_total / dt) * 100.0
#                 ram_mb = ram_bytes_total / (1024 * 1024)

#                 self.cpu_samples.put(cpu_pct)
#                 self.ram_samples.put(ram_mb)

#                 last_cpus = current_cpus
#                 last_time = now

#             except Exception:
#                 break

#     def stop(self):
#         self.stop_event.set()

# class ResourceMonitor(threading.Thread):
#     def __init__(self, total_ram_mb, interval=0.1):
#         super().__init__()
#         self.daemon = True
#         self.interval = interval
#         self.total_ram_mb = total_ram_mb
#         self.stop_event = threading.Event()
#         self.ram_samples_mb = []
#         self.ram_samples_pct = []
#         self.cpu_samples_pct = []
#         self.main_process = None
#         self.monitor_tid = None

#     def run(self):
#         try:
#             self.main_process = psutil.Process(os.getpid())
#             self.monitor_tid = threading.get_native_id()
#         except Exception:
#             return

#         last_threads_cpu = {}
#         last_children_cpu = {}  
#         last_time = time()

#         try:
#             for t in self.main_process.threads():
#                 last_threads_cpu[t.id] = t.user_time + t.system_time
#         except Exception:
#             pass

#         while not self.stop_event.is_set():
#             try:
#                 if self.main_process is None or not self.main_process.is_running():
#                     break

#                 now = time()
#                 dt = now - last_time
#                 if dt <= 0:
#                     dt = 0.0001

#                 # =========================================================
#                 # 1. PROCESSOS PAI (THREADS) - FILTRA A THREAD DO MONITOR
#                 # =========================================================
#                 cpu_pai_tempo = 0.0
#                 current_threads_cpu = {}

#                 try:
#                     for t in self.main_process.threads():
#                         tid = t.id
#                         curr_cpu = t.user_time + t.system_time
#                         current_threads_cpu[tid] = curr_cpu

#                         # Ignora a thread do monitor
#                         if tid == self.monitor_tid:
#                             continue

#                         prev_cpu = last_threads_cpu.get(tid, curr_cpu)
#                         cpu_pai_tempo += max(0.0, curr_cpu - prev_cpu)
#                 except (psutil.NoSuchProcess, psutil.AccessDenied):
#                     pass

#                 # =========================================================
#                 # 2. PROCESSOS FILHO - MEDIÇÃO DIRETA DE TEMPO
#                 # =========================================================
#                 cpu_filhos_tempo = 0.0
#                 current_children_cpu = {}
#                 ram_mb = self.main_process.memory_info().rss / (1024 * 1024)

#                 try:
#                     for child in self.main_process.children(recursive=True):
#                         try:
#                             if child.is_running():
#                                 pid = child.pid
#                                 # Pega o tempo total acumulado em segundos de CPU do filho
#                                 c_times = child.cpu_times()
#                                 curr_child_cpu = c_times.user + c_times.system
#                                 current_children_cpu[pid] = curr_child_cpu

#                                 prev_child_cpu = last_children_cpu.get(pid, curr_child_cpu)
#                                 cpu_filhos_tempo += max(0.0, curr_child_cpu - prev_child_cpu)

#                                 # Memória RAM
#                                 ram_mb += child.memory_info().rss / (1024 * 1024)
#                         except (psutil.NoSuchProcess, psutil.AccessDenied):
#                             continue
#                 except Exception:
#                     pass

#                 # =========================================================
#                 # 3. CÁLCULO DA PORCENTAGEM DE CPU (1 Núcleo Cheio = 100%)
#                 # =========================================================
#                 tempo_cpu_total = cpu_pai_tempo + cpu_filhos_tempo
#                 cpu_atual = (tempo_cpu_total / dt) * 100.0

#                 # Atualiza os estados anteriores para a próxima iteração
#                 last_threads_cpu = current_threads_cpu
#                 last_children_cpu = current_children_cpu
#                 last_time = now

#                 ram_pct = (ram_mb / self.total_ram_mb) * 100.0
#                 self.ram_samples_mb.append(ram_mb)
#                 self.ram_samples_pct.append(ram_pct)
#                 self.cpu_samples_pct.append(cpu_atual)

#             except Exception:
#                 break
                
#             sleep(self.interval)

#     def stop(self):
#         self.stop_event.set()


def get_heat_color(iter_count, max_iter):
    if iter_count == max_iter:
        return (0, 0, 0)
    
    t = math.log(iter_count + 1) / math.log(max_iter + 1)
    t = 1.0 - t

    r = int((math.sin(t * 3.0 + 0.0) * 0.5 + 0.5) * 255)
    g = int((math.sin(t * 3.0 + 1.5) * 0.5 + 0.5) * 255)
    b = int((math.sin(t * 3.0 + 3.0) * 0.5 + 0.5) * 255)
    
    return (r, g, b)


class Environment:
    def __init__(self, executor_type="thread", x_res=800, y_res=600, num_workers=1, num_blocks_side=4, max_iter=100):
        self.executor = None
        self.x_res = x_res
        self.y_res = y_res
        self.num_workers = num_workers
        self.num_blocks_side = num_blocks_side  
        self.max_iter = max_iter
        self.set_executor(executor_type)

    def set_executor(self, arg: str):
        if arg == 'process':
            self.executor = ProcessPoolExecutor(max_workers=self.num_workers)
        elif arg == 'thread':
            self.executor = ThreadPoolExecutor(max_workers=self.num_workers)
        else:
            raise ValueError("Executor não definido")


def divide_blocks(num_blocks_side, x_res, y_res):
    blocks = []
    for i in range(num_blocks_side):
        x_start = (i * x_res) // num_blocks_side
        x_end = ((i + 1) * x_res) // num_blocks_side if i < num_blocks_side - 1 else x_res
        
        for j in range(num_blocks_side):
            y_start = (j * y_res) // num_blocks_side
            y_end = ((j + 1) * y_res) // num_blocks_side if j < num_blocks_side - 1 else y_res
            
            coords = {
                "left_width": x_start,
                "right_width": x_end - 1,  
                "left_height": y_start,
                "right_height": y_end - 1  
            }
            blocks.append(coords)
            
    return blocks


def worker_task(block, max_iter, x_res, y_res):
    width = block["right_width"] - block["left_width"] + 1
    height = block["right_height"] - block["left_height"] + 1
    sub_matrix = [[0] * height for _ in range(width)]
    
    for a in range(block["left_width"], block["right_width"] + 1):
        for b in range(block["left_height"], block["right_height"] + 1):
            
            ca = (a - x_res / 2.0) * 4.0 / x_res
            cb = (b - y_res / 2.0) * 4.0 / y_res
            
            x, y = 0.0, 0.0
            iter_count = 0
            
            # Loop otimizado sem chamadas de função intermediárias
            while iter_count < max_iter:
                x_old = x
                x = x**2 - y**2 + ca
                y = 2 * x_old * y + cb
                if (x*x + y*y) > 4: 
                    break
                iter_count += 1
            
            local_a = a - block["left_width"]
            local_b = b - block["left_height"]
            
            sub_matrix[local_a][local_b] = get_heat_color(iter_count, max_iter)
                
    return block, sub_matrix


def run_single_mandelbrot_worker(args):
    """Executa a medição de uma réplica do mandelbrot e calcula médias contínuas de CPU e RAM."""
    t0 = time()
    total_ram_mb = psutil.virtual_memory().total / (1024 * 1024)

    monitor = ExternalResourceMonitor(target_pid=os.getpid(), interval=0.02)
    monitor.start()

    env = Environment(
        executor_type=args.executor, 
        x_res=1024, y_res=768,
        num_blocks_side=args.blocks, 
        num_workers=args.workers, 
        max_iter=200
    )
    
    blocks = divide_blocks(env.num_blocks_side, env.x_res, env.y_res)
    
    results = env.executor.map(worker_task, blocks, repeat(env.max_iter), repeat(env.x_res), repeat(env.y_res))
    results = list(results) 
    
    # Finaliza os workers
    env.executor.shutdown(wait=True)

    # Interrompe o monitor
    monitor.stop()
    monitor.join(timeout=2.0)

    tf = time() - t0

    cpu_samples_pct = []
    while not monitor.cpu_samples.empty():
        cpu_samples_pct.append(monitor.cpu_samples.get())

    ram_samples_mb = []
    while not monitor.ram_samples.empty():
        ram_samples_mb.append(monitor.ram_samples.get())

    # 4. Calcula porcentagens e médias
    ram_samples_pct = [(mb / total_ram_mb) * 100 for mb in ram_samples_mb]

    ram_mb_media = sum(ram_samples_mb) / len(ram_samples_mb) if ram_samples_mb else 0.0
    cpu_pct_media = sum(cpu_samples_pct) / len(cpu_samples_pct) if cpu_samples_pct else 0.0
    ram_pct_media = sum(ram_samples_pct) / len(ram_samples_pct) if ram_samples_pct else 0.0

    # Retorna para o orquestrador
    print(f"RESULT:{tf:.6f},{cpu_pct_media:.2f},{ram_pct_media:.2f},{ram_mb_media:.2f}", flush=True)

# Orquestrador do benchmark


initial_number_of_workers = 1
max_number_of_workers = os.cpu_count() or 4

factores = {
    "python_impl": ["free-threaded", "traditional"],
    "mechanism": ["thread", "process"],
    "workers": [initial_number_of_workers, 2, 4, max_number_of_workers],
}

experimentos = list(itertools.product(
    factores["python_impl"],
    factores["mechanism"],
    factores["workers"]
))

def executar_replica_orquestrada(exe_target, mech, workers):
    """Invoca o próprio arquivo e coleta os dados calculados internamente."""
    cmd = [
        exe_target, __file__,
        "--worker-mode",
        "--executor", mech,
        "--workers", str(workers),
        "--blocks", str(workers)
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"Erro na execução do Worker (Código {proc.returncode}):\n{stderr}")

    tempo_exec, cpu_pct, ram_pct, ram_mb = 0.0, 0.0, 0.0, 0.0
    for line in stdout.splitlines():
        if line.startswith("RESULT:"):
            parts = line.replace("RESULT:", "").split(",")
            tempo_exec = float(parts[0])
            cpu_pct = float(parts[1])
            ram_pct = float(parts[2])
            ram_mb = float(parts[3])
            break

    return tempo_exec, cpu_pct, ram_pct, ram_mb


def run_benchmark_orchestrator():
    files = {
        "free-threaded": "mandelbrot_freethreaded.csv",
        "traditional": "mandelbrot_traditional.csv"
    }

    # Garante a criação dos cabeçalhos CSV
    for impl, filename in files.items():
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "implementacao", "mecanismo", "workers", 
                    "replicacao", "tempo_execucao_s", "cpu_media_porcentagem", "ram_media_porcentagem", "ram_mb"
                ])

    total_cenarios = len(experimentos)

    for idx, (impl, mech, workers) in enumerate(experimentos, 1):
        exe_target = ENV_FREETHREADED if impl == 'free-threaded' else ENV_TRADITIONAL
        csv_target = files[impl]

        print("\n" + "="*60)
        print(f"[{idx}/{total_cenarios}] Executando Cenário: {impl}-{mech}-w{workers}")
        print("="*60, flush=True)

        try:
            with open(csv_target, mode='a', newline='') as f:
                writer = csv.writer(f)
                for rep in range(1, NUM_REPLICACOES + 1):
                    tempo, cpu_pct, ram_pct, ram_mb = executar_replica_orquestrada(exe_target, mech, workers)

                    writer.writerow([
                        impl, mech, workers, 
                        rep, 
                        f"{tempo:.6f}",
                        f"{cpu_pct:.2f}",
                        f"{ram_pct:.2f}",
                        f"{ram_mb:.2f}"
                    ])
                    f.flush()

                    print(f"  Réplica {rep}/{NUM_REPLICACOES} | Tempo: {tempo:.4f}s | CPU Média: {cpu_pct:.2f}% | RAM Média: {ram_pct:.2f}% | RAM (MB): {ram_mb:.2f}", flush=True)

        except KeyboardInterrupt:
            print("\nExecução interrompida pelo usuário.", flush=True)
            break
        except Exception as e:
            print(f"\nFalha no cenário {impl}-{mech}-w{workers}: {e}", flush=True)
            continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Mandelbrot Benchmark & Profiling")
    parser.add_argument('--worker-mode', action='store_true',)
    parser.add_argument('--executor', choices=['thread', 'process'], required=False)
    parser.add_argument('--workers', type=int, required=False)
    parser.add_argument('--blocks', type=int, required=False)

    args = parser.parse_args()

    if args.worker_mode:
        run_single_mandelbrot_worker(args)
    else:
        run_benchmark_orchestrator()