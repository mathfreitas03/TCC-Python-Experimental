import argparse
import csv
import itertools
import multiprocessing as mp
import os
import queue
import subprocess
import sys
import threading
import time
from resource_monitor import ExternalResourceMonitor
import psutil
from dataclasses import dataclass

ENV_TRADITIONAL = os.path.join("venv_traditional", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_traditional", "bin", "python")
ENV_FREETHREADED = os.path.join("venv_free_threaded", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_free_threaded", "bin", "python")

NUM_REPLICACOES = 10

# carga de trabalho total para Strong Scaling
TOTAL_ITEMS_STARK_SCALING = 800000

@dataclass(slots=True, frozen=True)
class Item:
    timestamp_ns: int
    payload: bytes

def producer(worker_id, q, num_items, item_payload):
    for _ in range(num_items):
        # Carimbo de tempo individual por item para medir o tempo exato em fila
        item = Item(timestamp_ns=time.perf_counter_ns(), payload=item_payload)
        q.put(item)

def consumer(worker_id, q, metrics_q):
    total_latency_ns = 0
    items_count = 0
    while True:
        item = q.get()
        if item is None:
            break
        latency_ns = time.perf_counter_ns() - item.timestamp_ns
        total_latency_ns += latency_ns
        items_count += 1
    
    # Retorna o acumulado e a contagem do consumidor
    metrics_q.put((total_latency_ns, items_count))

class Environment:
    def __init__(self, mecanismo, num_producers, num_consumers, items_per_producer, item_size_bytes):
        self.mecanismo = mecanismo
        self.num_producers = num_producers  
        self.items_per_producer = items_per_producer
        self.num_consumers = num_consumers
        self.item_size = item_size_bytes

        # Limitar o tamanho da fila previne estouros de memória e contenção
        if self.mecanismo == "thread":
            self.queue = queue.Queue(maxsize=10000)
            self.metrics_queue = queue.Queue()
            self.worker = threading.Thread
        else:
            self.queue = mp.Queue(maxsize=10000)
            self.metrics_queue = mp.Queue()
            self.worker = mp.Process

    def run(self):
        payload = b"x" * self.item_size
        producers = []
        consumers = []

        start_time = time.perf_counter()

        for i in range(self.num_consumers):
            w = self.worker(target=consumer, args=(i, self.queue, self.metrics_queue))
            consumers.append(w)
            w.start()

        for i in range(self.num_producers):
            w = self.worker(target=producer, args=(i, self.queue, self.items_per_producer, payload))
            producers.append(w)
            w.start()

        for p in producers:
            p.join()

        # Poison pill para finalizar os consumidores
        for _ in range(self.num_consumers):
            self.queue.put(None)

        for c in consumers:
            c.join()
            
        end_time = time.perf_counter()
        
        # Coleta das métricas acumuladas de latência
        total_queue_latency_ns = 0
        total_consumed_items = 0
        
        for _ in range(self.num_consumers):
            lat_ns, count = self.metrics_queue.get()
            total_queue_latency_ns += lat_ns
            total_consumed_items += count

        duration = end_time - start_time
        total_items = self.num_producers * self.items_per_producer
        total_bytes = total_items * self.item_size

        throughput_msg_s = total_items / duration if duration > 0 else 0.0
        throughput_mb_s = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0.0

        # Conversão de latência para milissegundos (ms)
        total_latency_ms = total_queue_latency_ns / 1e6
        avg_latency_ms = (total_queue_latency_ns / total_consumed_items / 1e6) if total_consumed_items > 0 else 0.0

        return duration, throughput_msg_s, throughput_mb_s, avg_latency_ms, total_latency_ms

def run_single_producer_consumer_worker(args):
    """Executa uma réplica do benchmark isoladamente e imprime a métrica serializada."""
    total_ram_mb = psutil.virtual_memory().total / (1024 * 1024)

    # Inicia o monitor de recursos
    monitor = ExternalResourceMonitor(target_pid=os.getpid(), interval=0.02)
    monitor.start()

    env = Environment(
        mecanismo=args.mecanismo,
        num_producers=args.producers,
        num_consumers=args.consumers,
        items_per_producer=args.items,
        item_size_bytes=args.size
    )

    duration, throughput_msg_s, throughput_mb_s, avg_lat_ms, total_lat_ms = env.run()

    # Interrompe o monitor e drena as amostras
    monitor.stop()
    monitor.join(timeout=2.0)

    cpu_samples = []
    while not monitor.cpu_samples.empty():
        cpu_samples.append(monitor.cpu_samples.get())

    ram_samples_mb = []
    while not monitor.ram_samples.empty():
        ram_samples_mb.append(monitor.ram_samples.get())

    cpu_pct_media = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    ram_mb_media = sum(ram_samples_mb) / len(ram_samples_mb) if ram_samples_mb else 0.0
    ram_pct_media = (ram_mb_media / total_ram_mb) * 100.0 if total_ram_mb > 0 else 0.0

    # Imprime a linha de resultado capturada pelo orquestrador
    print(
        f"RESULT:{duration:.6f},{throughput_msg_s:.2f},{throughput_mb_s:.2f},"
        f"{avg_lat_ms:.6f},{total_lat_ms:.6f},"
        f"{cpu_pct_media:.2f},{ram_mb_media:.2f},{ram_pct_media:.2f}",
        flush=True
    )


factores = {
    "python_impl": ["traditional"], # , free-threaded]
    "mechanism": ["thread", "multiprocess"],
    "producers": [1, 2, 4, 8],
    "consumers": [1, 2, 4, 8],
    "item_size_bytes": [1, 65536]
}

experimentos = list(itertools.product(
    factores["python_impl"],
    factores["mechanism"],
    factores["producers"],
    factores["consumers"],
    factores["item_size_bytes"]
))

def executar_replica_orquestrada(exe_target, mech, prod, cons, items, size):
    """Invoca a réplica worker usando o interpretador do venv específico."""
    cmd = [
        exe_target, __file__,
        "--worker-mode",
        "--mecanismo", mech,
        "--producers", str(prod),
        "--consumers", str(cons),
        "--items", str(items),
        "--size", str(size)
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"Erro na execução do Worker (Código {proc.returncode}):\n{stderr}")

    duration, throughput_msg, throughput_mb = 0.0, 0.0, 0.0
    avg_lat, total_lat = 0.0, 0.0
    cpu_pct, ram_mb, ram_pct = 0.0, 0.0, 0.0

    for line in stdout.splitlines():
        if line.startswith("RESULT:"):
            parts = line.replace("RESULT:", "").split(",")
            duration = float(parts[0])
            throughput_msg = float(parts[1])
            throughput_mb = float(parts[2])
            avg_lat = float(parts[3])
            total_lat = float(parts[4])
            cpu_pct = float(parts[5])
            ram_mb = float(parts[6])
            ram_pct = float(parts[7])
            break

    return duration, throughput_msg, throughput_mb, avg_lat, total_lat, cpu_pct, ram_mb, ram_pct

def run_benchmark_orchestrator():
    files = {
        "free-threaded": "produtor_consumidor_freethreaded_strong.csv",
        "traditional": "produtor_consumidor_traditional_strong.csv"
    }

    # inicializacao de cabeçalhos
    
    for impl, filename in files.items():
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "implementacao", "mecanismo", "producers", "consumers", 
                    "items_per_producer", "item_size_bytes", "total_items", "total_mbytes", "replicacao",
                    "tempo_execucao_s", "throughput_msg_s", "throughput_mb_s", 
                    "queue_latency_media_ms", "queue_latency_total_ms",
                    "cpu_media_porcentagem", "ram_media_mb", "ram_media_porcentagem"
                ])

    total_cenarios = len(experimentos)

    for idx, (impl, mech, prod, cons, size) in enumerate(experimentos, 1):
        exe_target = ENV_FREETHREADED if impl == 'free-threaded' else ENV_TRADITIONAL
        csv_target = files[impl]
        
        total_items = TOTAL_ITEMS_STARK_SCALING
        items_per_producer = total_items // prod
        total_mbytes = (total_items * size) / (1024 * 1024)

        print("\n" + "="*70)
        print(f"[{idx}/{total_cenarios}] Strong Scaling | {impl.upper()} | Mech: {mech} | P: {prod} | C: {cons} | Total Items: {total_items} | Size: {size}B")
        print("="*70, flush=True)

        try:
            with open(csv_target, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for rep in range(1, NUM_REPLICACOES + 1):
                    (duration, throughput_msg, throughput_mb, 
                     avg_lat, total_lat, cpu_pct, ram_mb, ram_pct) = executar_replica_orquestrada(
                        exe_target, mech, prod, cons, items_per_producer, size
                    )

                    writer.writerow([
                        impl, mech, prod, cons, items_per_producer, size, total_items, f"{total_mbytes:.2f}", rep,
                        f"{duration:.6f}", f"{throughput_msg:.2f}", f"{throughput_mb:.2f}",
                        f"{avg_lat:.6f}", f"{total_lat:.6f}",
                        f"{cpu_pct:.2f}", f"{ram_mb:.2f}", f"{ram_pct:.2f}"
                    ])
                    f.flush()

                    print(
                        f"  Réplica {rep}/{NUM_REPLICACOES} | Tempo: {duration:.4f}s | "
                        f"Throughput: {throughput_mb:.2f} MB/s ({throughput_msg:.2f} msg/s) | "
                        f"Latência Média: {avg_lat:.4f}ms | CPU: {cpu_pct:.2f}% | RAM: {ram_mb:.2f}MB ({ram_pct:.2f}%)",
                        flush=True
                    )

        except KeyboardInterrupt:
            print("\nBenchmark interrompido pelo usuário.", flush=True)
            break
        except Exception as e:
            print(f"\nFalha no cenário {impl}-{mech}: {e}", flush=True)
            continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Produtor-Consumidor Benchmark Orchestrator")
    parser.add_argument('--worker-mode', action='store_true')
    parser.add_argument('-m', '--mecanismo', choices=['thread', 'multiprocess'], required=False)
    parser.add_argument('-p', '--producers', type=int, required=False)
    parser.add_argument('-c', '--consumers', type=int, required=False)
    parser.add_argument('-n', '--items', type=int, required=False)
    parser.add_argument('-s', '--size', type=int, required=False)

    args = parser.parse_args()

    if args.worker_mode:
        run_single_producer_consumer_worker(args)
    else:
        run_benchmark_orchestrator()