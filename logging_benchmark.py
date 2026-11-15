import argparse
import csv
import io
import itertools
import logging
from logging.handlers import QueueHandler, QueueListener
import multiprocessing as mp
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
import psutil
from resource_monitor import ExternalResourceMonitor
from itertools import cycle

logging._srcfile = None
logging.logThreads = False
logging.logProcesses = False
logging.logMultiprocessing = False
logging.logAsyncioTasks = False

MOCK_MAP = {
    "G": Path("./mocks/G.csv"),
}

LOG_DIR = Path("./logs")

def load_mock_pool(mock_path: Path) -> list:
    """Carrega as amostras do arquivo mock."""
    if not mock_path.exists():
        return [{"level": "INFO", "message": f"Sample log record {i}"} for i in range(100)]

    if mock_path.suffix.lower() == ".csv":
        with open(mock_path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    else:
        import json
        with open(mock_path, "r", encoding="utf-8") as f:
            return json.load(f)

# QUEUE Simples (Produtor coloca na fila única) ---
def queue_logging_producer(worker_id, log_queue, num_items, sample_pool):
    logger = logging.getLogger(f"queue_producer_{worker_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    qh = QueueHandler(log_queue)
    logger.addHandler(qh)

    pool_len = len(sample_pool)
    for i in range(num_items):
        payload = sample_pool[i % pool_len]
        logger.info(payload)

    logger.removeHandler(qh)


# Modo MULTIFILAS (Produtor conectado a TODAS as filas de forma determinística e balanceada) ---
def multiqueue_logging_producer(worker_id, log_queues, num_items, sample_pool):
    logger = logging.getLogger(f"multiqueue_producer_{worker_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    # Cria QueueHandlers para todas as filas do sistema
    qhs = [QueueHandler(q) for q in log_queues]
    for qh in qhs:
        logger.addHandler(qh)

    pool_len = len(sample_pool)
    num_queues = len(log_queues)

    for i in range(num_items):
        payload = sample_pool[i % pool_len]
        # Round-Robin entre todas as filas
        target_queue_idx = i % num_queues
        
        record = logger.makeRecord(
            logger.name, logging.INFO, "", 0, payload, (), None
        )
        qhs[target_queue_idx].emit(record)

    for qh in qhs:
        logger.removeHandler(qh)


def queue_logging_consumer(worker_id, log_queue, metrics_q):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_path = LOG_DIR / f"consumer_worker_{worker_id}.log"

    logger = logging.getLogger(f"consumer_logger_{worker_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.FileHandler(file_path, mode="w", encoding="utf-8")
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    records_count = 0
    total_latency_ns = 0

    while True:
        record = log_queue.get()
        if record is None:
            break
        
        t_start = time.perf_counter_ns()
        logger.handle(record)
        total_latency_ns += (time.perf_counter_ns() - t_start)
        records_count += 1

    handler.flush()
    handler.close()
    logger.removeHandler(handler)

    total_bytes = file_path.stat().st_size if file_path.exists() else 0
    metrics_q.put((total_latency_ns, records_count, total_bytes))


class Environment:
    def __init__(self, mecanismo, num_producers, num_consumers, total_items, sample_pool):
        self.mecanismo = mecanismo
        self.num_producers = num_producers  
        self.num_consumers = num_consumers
        self.sample_pool = sample_pool
        self.total_items = total_items
        self.items_per_producer = total_items // num_producers

    def run(self):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        start_time = time.perf_counter()

        if self.mecanismo in ["listener", "mp_listener"]:
            listener_log_file = LOG_DIR / f"{self.mecanismo}_output.log"
            handler = logging.FileHandler(listener_log_file, mode="w", encoding="utf-8")
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)

            if self.mecanismo == "listener":
                log_q = queue.Queue(maxsize=10000)
                worker_cls = threading.Thread
            else:
                log_q = mp.Queue(maxsize=10000)
                worker_cls = mp.Process

            listener = QueueListener(log_q, handler, respect_handler_level=True)
            listener.start()

            producers = []
            for i in range(self.num_producers):
                p = worker_cls(
                    target=queue_logging_producer,
                    args=(i, log_q, self.items_per_producer, self.sample_pool)
                )
                producers.append(p)
                p.start()

            for p in producers:
                p.join()

            listener.stop()
            handler.close()

            total_lat_ns = 0
            total_consumed_items = self.items_per_producer * self.num_producers
            total_bytes_processed = listener_log_file.stat().st_size if listener_log_file.exists() else 0

        elif self.mecanismo in ["multiqueue_threads", "multiqueue_mp"]:
            if self.mecanismo == "multiqueue_threads":
                worker_cls = threading.Thread
                metrics_queue = queue.Queue()
                num_queues = max(self.num_consumers, 1)
                log_queues = [queue.Queue(maxsize=10000) for _ in range(num_queues)]
            else:
                worker_cls = mp.Process
                metrics_queue = mp.Queue()
                num_queues = max(self.num_consumers, 1)
                log_queues = [mp.Queue(maxsize=10000) for _ in range(num_queues)]

            consumers = []
            producers = []

            for i in range(num_queues):
                w = worker_cls(target=queue_logging_consumer, args=(i, log_queues[i], metrics_queue))
                consumers.append(w)
                w.start()

            for i in range(self.num_producers):
                w = worker_cls(target=multiqueue_logging_producer, args=(i, log_queues, self.items_per_producer, self.sample_pool))
                producers.append(w)
                w.start()

            for p in producers:
                p.join()

            for i in range(num_queues):
                log_queues[i].put(None)

            for c in consumers:
                c.join()

            total_lat_ns, total_consumed_items, total_bytes_processed = 0, 0, 0
            for _ in range(num_queues):
                lat_ns, count, b_count = metrics_queue.get()
                total_lat_ns += lat_ns
                total_consumed_items += count
                total_bytes_processed += b_count

        else:
            if self.mecanismo == "thread":
                log_q = queue.Queue(maxsize=10000)
                metrics_queue = queue.Queue()
                worker_cls = threading.Thread
            else:
                log_q = mp.Queue(maxsize=10000)
                metrics_queue = mp.Queue()
                worker_cls = mp.Process

            consumers = []
            producers = []

            for i in range(self.num_consumers):
                w = worker_cls(target=queue_logging_consumer, args=(i, log_q, metrics_queue))
                consumers.append(w)
                w.start()

            for i in range(self.num_producers):
                w = worker_cls(target=queue_logging_producer, args=(i, log_q, self.items_per_producer, self.sample_pool))
                producers.append(w)
                w.start()

            for p in producers:
                p.join()

            for _ in range(self.num_consumers):
                log_q.put(None)

            for c in consumers:
                c.join()

            total_lat_ns, total_consumed_items, total_bytes_processed = 0, 0, 0
            for _ in range(self.num_consumers):
                lat_ns, count, b_count = metrics_queue.get()
                total_lat_ns += lat_ns
                total_consumed_items += count
                total_bytes_processed += b_count

        end_time = time.perf_counter()

        duration = end_time - start_time
        real_total_items = self.items_per_producer * self.num_producers

        throughput_msg_s = real_total_items / duration if duration > 0 else 0.0
        throughput_mb_s = (total_bytes_processed / (1024 * 1024)) / duration if duration > 0 else 0.0
        avg_latency_ms = (total_lat_ns / total_consumed_items / 1e6) if total_consumed_items > 0 else 0.0
        total_latency_ms = total_lat_ns / 1e6

        return duration, throughput_msg_s, throughput_mb_s, avg_latency_ms, total_latency_ms


def run_single_logging_worker(args):
    total_ram_mb = psutil.virtual_memory().total / (1024 * 1024)

    monitor = ExternalResourceMonitor(target_pid=os.getpid(), interval=0.02)
    monitor.start()

    mock_file = MOCK_MAP.get(args.payload, Path(f"./mocks/payload_{args.payload.lower()}.json"))
    sample_pool = load_mock_pool(mock_file)

    env = Environment(
        mecanismo=args.mecanismo,
        num_producers=args.producers,
        num_consumers=args.consumers,
        total_items=args.total_items,
        sample_pool=sample_pool
    )

    duration, throughput_msg_s, throughput_mb_s, avg_lat_ms, total_lat_ms = env.run()

    monitor.stop()
    monitor.join(timeout=2.0)

    cpu_samples = []
    while not monitor.cpu_samples.empty():
        cpu_samples.append(monitor.cpu_samples.get())

    ram_samples_mb = []
    while not monitor.ram_samples.empty():
        ram_samples_mb.append(monitor.ram_samples.get())

    io_samples_mb_s = []
    while not monitor.io_samples.empty():
        io_samples_mb_s.append(monitor.io_samples.get())

    cpu_pct_media = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    ram_mb_media = sum(ram_samples_mb) / len(ram_samples_mb) if ram_samples_mb else 0.0
    ram_pct_media = (ram_mb_media / total_ram_mb) * 100.0 if total_ram_mb > 0 else 0.0
    io_mb_s_media = sum(io_samples_mb_s) / len(io_samples_mb_s) if io_samples_mb_s else 0.0

    print(
        f"RESULT:{duration:.6f},{throughput_msg_s:.2f},{throughput_mb_s:.2f},"
        f"{avg_lat_ms:.6f},{total_lat_ms:.6f},"
        f"{cpu_pct_media:.2f},{ram_mb_media:.2f},{ram_pct_media:.2f},{io_mb_s_media:.2f}",
        flush=True
    )


# -----------------------------------------------------------------------------
# ORQUESTRADOR
# -----------------------------------------------------------------------------

ENV_TRADITIONAL = os.path.join("venv_traditional", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_traditional", "bin", "python")
ENV_FREETHREADED = os.path.join("venv_free_threaded", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_free_threaded", "bin", "python")

NUM_REPLICACOES = 5

factores = {
    "python_impl": ["free-threaded", "traditional"],
    "mechanism": ["multiqueue_threads", "multiqueue_mp", "thread", "multiprocess"],
    "producers": [1, 2, 4, 8],
    "consumers": [1, 2, 4, 8],
    "total_items": [800000],
    "payload": "G"
}

experimentos = list(itertools.product(
    factores["python_impl"],
    factores["mechanism"],
    factores["producers"],
    factores["consumers"],
    factores["total_items"],
    factores["payload"]
))

def executar_replica_orquestrada(exe_target, mech, prod, cons, total_items, payload):
    cmd = [
        exe_target, __file__,
        "--worker-mode",
        "--mecanismo", mech,
        "--producers", str(prod),
        "--consumers", str(cons),
        "--total-items", str(total_items),
        "--payload", payload
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"Erro na execução do Worker (Código {proc.returncode}):\n{stderr}")

    duration, throughput_msg, throughput_mb = 0.0, 0.0, 0.0
    avg_lat, total_lat = 0.0, 0.0
    cpu_pct, ram_mb, ram_pct, io_mb_s_media = 0.0, 0.0, 0.0, 0.0

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
            io_mb_s_media = float(parts[8])
            break

    return duration, throughput_msg, throughput_mb, avg_lat, total_lat, cpu_pct, ram_mb, ram_pct, io_mb_s_media

def run_benchmark_orchestrator():
    files = {
        "free-threaded": "logging_freethreaded.csv",
        "traditional": "logging_traditional.csv"
    }

    for impl, filename in files.items():
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "implementacao", "mecanismo", "producers", "consumers", 
                    "total_items", "payload_type", "replicacao",
                    "tempo_execucao_s", "throughput_msg_s", "throughput_mb_s",
                    "queue_latency_media_ms", "queue_latency_total_ms",
                    "cpu_media_porcentagem", "ram_media_mb", "ram_media_porcentagem",
                    "io_write_media_mb_s"
                ])

    total_cenarios = len(experimentos)

    for idx, (impl, mech, prod, cons, total_items, payload) in enumerate(experimentos, 1):
        if mech in ["listener", "mp_listener"] and cons > 1:
            continue

        exe_target = ENV_FREETHREADED if impl == 'free-threaded' else ENV_TRADITIONAL
        csv_target = files[impl]

        print("\n" + "="*70)
        print(f"[{idx}/{total_cenarios}] Strong Scaling (FileHandler I/O) | Impl: {impl.upper()} | Mech: {mech} | P: {prod} | C: {1 if mech in ['listener', 'mp_listener'] else cons} | Total Items: {total_items} | Payload: {payload}")
        print("="*70, flush=True)

        try:
            with open(csv_target, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for rep in range(1, NUM_REPLICACOES + 1):
                    print(f"  [>] Iniciando Réplica {rep}/{NUM_REPLICACOES}...", end="", flush=True)
                    
                    (duration, throughput_msg, throughput_mb, avg_lat, total_lat, 
                     cpu_pct, ram_mb, ram_pct, io_mb_s_media) = executar_replica_orquestrada(
                        exe_target, mech, prod, cons, total_items, payload
                    )

                    c_val = 1 if mech in ["listener", "mp_listener"] else cons

                    writer.writerow([
                        impl, mech, prod, c_val, total_items, payload, rep,
                        f"{duration:.6f}", f"{throughput_msg:.2f}", f"{throughput_mb:.2f}",
                        f"{avg_lat:.6f}", f"{total_lat:.6f}",
                        f"{cpu_pct:.2f}", f"{ram_mb:.2f}", f"{ram_pct:.2f}",
                        f"{io_mb_s_media:.2f}"
                    ])
                    f.flush()

                    print(
                        f"\r  [✓] Réplica {rep}/{NUM_REPLICACOES} concluída | Tempo: {duration:.4f}s | "
                        f"Vazão: {throughput_msg:.2f} msg/s | I/O SO: {io_mb_s_media:.2f} MB/s | "
                        f"CPU: {cpu_pct:.2f}%",
                        flush=True
                    )

        except KeyboardInterrupt:
            print("\nBenchmark interrompido pelo usuário.", flush=True)
            break
        except Exception as e:
            print(f"\nFalha no cenário {impl}-{mech}: {e}", flush=True)
            continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Logging Benchmark Orchestrator (FileHandler I/O + External Resource Monitor)")
    parser.add_argument('--worker-mode', action='store_true')
    parser.add_argument('-m', '--mecanismo', choices=['multiqueue_threads', 'multiqueue_mp', 'listener', 'mp_listener', 'thread', 'multiprocess'], required=False)
    parser.add_argument('-p', '--producers', type=int, required=False)
    parser.add_argument('-c', '--consumers', type=int, required=False)
    parser.add_argument('-t', '--total-items', type=int, required=False)
    parser.add_argument('-l', '--payload', choices=['P', 'G'], required=False)

    args = parser.parse_args()

    if args.worker_mode:
        run_single_logging_worker(args)
    else:
        run_benchmark_orchestrator()