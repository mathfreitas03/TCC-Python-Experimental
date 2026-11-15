from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Thread, Event
from time import perf_counter
from pathlib import Path
import xml.etree.ElementTree as ET
import traceback
import sqlite3
import csv
import os
import time
from time import sleep
import multiprocessing as mp
import psutil
import argparse
import sys
import itertools
import sumolib
import traci
from resource_monitor import ExternalResourceMonitor

# Logica do pipeline

CFG_PATH = Path(r".\lust\LuSTScenario\scenario\dua.static.sumocfg").resolve()
NET_PATH = CFG_PATH.parent / "lust.net.xml"
SUMO_BINARY = sumolib.checkBinary("sumo")

_GRAFO = None

def log(msg):
    print(f"[{perf_counter():.3f}] {msg}", flush=True)

def preparar_cfg_dinamico(cfg_path, edge_data_output):
    tree = ET.parse(cfg_path)
    root = tree.getroot()

    for parent in root.iter():
        for child in list(parent):
            if child.tag == "route-files":
                parent.remove(child)

    add_path = cfg_path.with_name("_edge_data.add.xml")
    add_root = ET.Element("additional")
    edge_data_elem = ET.SubElement(add_root, "edgeData")
    edge_data_elem.set("id", "vazao_geral")
    edge_data_elem.set("freq", "60")
    edge_data_elem.set("file", str(edge_data_output.resolve()))
    
    add_tree = ET.ElementTree(add_root)
    add_tree.write(add_path, encoding="UTF-8", xml_declaration=True)

    input_elem = root.find("input")
    if input_elem is None:
        input_elem = ET.SubElement(root, "input")
    
    additional_files_elem = input_elem.find("additional-files")
    if additional_files_elem is None:
        additional_files_elem = ET.SubElement(input_elem, "additional-files")
        additional_files_elem.set("value", add_path.name)
    else:
        current_vals = additional_files_elem.get("value", "")
        additional_files_elem.set("value", f"{current_vals},{add_path.name}" if current_vals else add_path.name)

    cfg_temp = cfg_path.with_name("_python_dinamico.sumocfg")
    tree.write(cfg_temp, encoding="UTF-8", xml_declaration=True)
    return cfg_temp, add_path

def carregar_tarefas(cfg_path, limite_total=None):
    root = ET.parse(cfg_path).getroot()
    tarefas = {}

    route_files = []
    for elem in root.findall(".//route-files"):
        for filename in elem.get("value", "").split(","):
            filename = filename.strip()
            if filename:
                path = Path(filename)
                if not path.is_absolute():
                    path = cfg_path.parent / path
                route_files.append(path.resolve())

    for path in route_files:
        if not path.exists():
            continue
        for event, elem in ET.iterparse(str(path), events=("end",)):
            if elem.tag == "vehicle":
                route = elem.find("route")
                if route is not None:
                    edges = route.get("edges", "").split()
                    vid = elem.get("id")
                    if vid and len(edges) >= 2:
                        tarefas[vid] = (vid, edges[0], edges[-1], float(elem.get("depart", "0")))
                elem.clear()
            elif elem.tag == "trip":
                vid = elem.get("id")
                origem = elem.get("from")
                destino = elem.get("to")
                if vid and origem and destino:
                    tarefas[vid] = (vid, origem, destino, float(elem.get("depart", "0")))
                elem.clear()

    lista = list(tarefas.values())
    if limite_total:
        lista = lista[:limite_total]
    return lista

def inicializar_worker(net_path):
    global _GRAFO
    _GRAFO = sumolib.net.readNet(str(net_path))

def calcular_rota(tarefa):
    vid, origem_id, destino_id, depart = tarefa
    inicio = perf_counter()
    try:
        origem = _GRAFO.getEdge(origem_id)
        destino = _GRAFO.getEdge(destino_id)
        caminho, distancia = _GRAFO.getShortestPath(origem, destino)
        return {
            "id": vid, "origem": origem_id, "destino": destino_id, "depart": depart,
            "rota": [e.getID() for e in caminho] if caminho is not None else None,
            "distancia": distancia, "tempo_calculo_s": perf_counter() - inicio, "erro": None
        }
    except Exception as exc:
        return {
            "id": vid, "origem": origem_id, "destino": destino_id, "depart": depart,
            "rota": None, "distancia": None, "tempo_calculo_s": perf_counter() - inicio, "erro": repr(exc)
        }

def thread_db(db_path, fila_db, db_finalizado, eventos_fases):
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS simulacao_vazao (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tempo REAL,
                edge_id TEXT,
                vazao_veiculos INTEGER,
                velocidade_media REAL
            );
        """)
        conn.commit()

        lote = []
        while True:
            try:
                item = fila_db.get(timeout=0.5)
                if item is None:
                    break
                
                # Registra o momento em que o item chega à fila de banco de dados
                momento_chegada_fila = perf_counter()
                if isinstance(item, dict):
                    item["momento_chegada_fila_db"] = momento_chegada_fila

                lote.append(item)
                if len(lote) >= 500:
                    _inserir_lote(cursor, conn, lote)
                    lote.clear()
            except Empty:
                if db_finalizado.is_set() and fila_db.empty():
                    break
        if lote:
            _inserir_lote(cursor, conn, lote)
        cursor.close()
        conn.close()
        
        # Marca o fim da fase de persistência/DB
        eventos_fases["fim_persistencia"] = perf_counter()
    except Exception:
        traceback.print_exc()

def _inserir_lote(cursor, conn, lote):
    dados = [(x["tempo"], x["edge_id"], x["vazao_veiculos"], x["velocidade_media"]) for x in lote]
    cursor.executemany("INSERT INTO simulacao_vazao (tempo, edge_id, vazao_veiculos, velocidade_media) VALUES (?, ?, ?, ?)", dados)
    conn.commit()

def processar_ficheiro_edge_data(xml_path, fila_db):
    if not xml_path.exists():
        return
    tempo_atual = 0.0
    for event, elem in ET.iterparse(str(xml_path), events=("start", "end")):
        if event == "start" and elem.tag == "interval":
            tempo_atual = float(elem.get("end", 0))
        elif event == "end" and elem.tag == "edge":
            edge_id = elem.get("id")
            if float(elem.get("sampledSeconds", 0)) > 0:
                veh_number = int(elem.get("entered", 0))
                vel_media = float(elem.get("speed", 0.0))
                if veh_number > 0:
                    fila_db.put({"tempo": tempo_atual, "edge_id": edge_id, "vazao_veiculos": veh_number, "velocidade_media": vel_media})
            elem.clear()

def thread_traci(cfg_dinamico, tarefas, fila_rotas, workers_finalizados, serie_temporal_latencias, eventos_fases):
    prontos = {}
    processados = set()
    horarios = {t[0]: t[3] for t in tarefas}
    total = len(tarefas)
    inicio_simulacao = perf_counter()

    try:
        traci.start([SUMO_BINARY, "-c", str(cfg_dinamico), "--start"])
        while True:
            while True:
                try:
                    item_fila = fila_rotas.get_nowait()
                    momento_retirada_fila = perf_counter()

                    if isinstance(item_fila, dict) and "timestamp" in item_fila:
                        momento_chegada = momento_retirada_fila
                        lat_ms = (momento_chegada - item_fila["timestamp"]) * 1000.0
                        tempo_relativo = momento_chegada - inicio_simulacao
                        
                        serie_temporal_latencias.append((
                            tempo_relativo, 
                            lat_ms, 
                            item_fila.get("momento_chegada_fila_rotas", momento_chegada)
                        ))

                        for resultado in item_fila["lote"]:
                            prontos[resultado["id"]] = resultado
                    elif isinstance(item_fila, list):
                        for resultado in item_fila:
                            prontos[resultado["id"]] = resultado
                except Empty:
                    break

            agora = traci.simulation.getTime()
            delta = traci.simulation.getDeltaT()
            proximo_tempo = agora + delta

            devidos = [vid for vid, depart in horarios.items() if vid not in processados and depart <= proximo_tempo + 1e-9]

            if devidos and not workers_finalizados.is_set() and not all(vid in prontos for vid in devidos):
                try:
                    item_fila = fila_rotas.get(timeout=0.02)
                    momento_retirada_fila = perf_counter()

                    if isinstance(item_fila, dict) and "timestamp" in item_fila:
                        momento_chegada = momento_retirada_fila
                        lat_ms = (momento_chegada - item_fila["timestamp"]) * 1000.0
                        tempo_relativo = momento_chegada - inicio_simulacao
                        
                        serie_temporal_latencias.append((
                            tempo_relativo, 
                            lat_ms, 
                            item_fila.get("momento_chegada_fila_rotas", momento_chegada)
                        ))

                        for r in item_fila["lote"]:
                            prontos[r["id"]] = r
                    elif isinstance(item_fila, list):
                        for r in item_fila:
                            prontos[r["id"]] = r
                except Empty:
                    pass

            for vid in devidos:
                resultado = prontos.pop(vid, None)
                if resultado is None or not resultado["rota"]:
                    processados.add(vid)
                    continue

                try:
                    route_id = f"pyroute_{len(processados)}"
                    traci.route.add(route_id, resultado["rota"])
                    traci.vehicle.add(
                        vehID=vid, routeID=route_id, typeID="DEFAULT_VEHTYPE",
                        depart=str(max(agora, resultado["depart"])), departLane="first", departPos="base", departSpeed="0"
                    )
                except traci.TraCIException:
                    pass
                finally:
                    processados.add(vid)

            traci.simulationStep()

            if workers_finalizados.is_set() and fila_rotas.empty() and len(processados) == total and traci.simulation.getMinExpectedNumber() == 0:
                break
        
        # Marca o fim da fase de simulação TraCI
        eventos_fases["fim_simulacao"] = perf_counter()
    except Exception:
        traceback.print_exc()
    finally:
        try:
            traci.close()
        except Exception:
            pass

def executar_pipeline_worker(mecanismo, num_workers, total_items_limite, impl_nome="free-threaded"):
    global _GRAFO
    edge_data_output = Path(f"vazao_output_{os.getpid()}.xml")
    db_path = Path(f"simulacao_{os.getpid()}.db")

    tarefas = carregar_tarefas(CFG_PATH, limite_total=total_items_limite)
    if not tarefas:
        raise RuntimeError("Nenhuma tarefa encontrada.")
    tarefas.sort(key=lambda x: x[3])

    cfg_dinamico, add_path = preparar_cfg_dinamico(CFG_PATH, edge_data_output)

    if mecanismo == "thread":
        _GRAFO = sumolib.net.readNet(str(NET_PATH))
        executor_cls = ThreadPoolExecutor
        kwargs = {"max_workers": num_workers}
    else:
        executor_cls = ProcessPoolExecutor
        kwargs = {"max_workers": num_workers, "initializer": inicializar_worker, "initargs": (NET_PATH,)}

    fila_rotas = Queue()
    fila_db = Queue()
    workers_finalizados = Event()
    db_finalizado = Event()
    serie_temporal_latencias = []
    
    # Dicionário para registrar os timestamps exatos de término de cada fase
    eventos_fases = {
        "inicio_total": perf_counter(),
        "fim_roteamento": 0.0,
        "fim_simulacao": 0.0,
        "fim_persistencia": 0.0
    }

    monitor = ExternalResourceMonitor(target_pid=os.getpid(), interval=0.1)
    monitor.start()

    t_db = Thread(target=thread_db, args=(db_path, fila_db, db_finalizado, eventos_fases), name="Thread-DB")
    t_db.start()

    simulacao = Thread(
        target=thread_traci,
        args=(cfg_dinamico, tarefas, fila_rotas, workers_finalizados, serie_temporal_latencias, eventos_fases),
        name="Thread-TraCI",
    )
    simulacao.start()

    resultados = []
    lote_rotas = []
    tamanho_lote_rotas = 50
    try:
        with executor_cls(**kwargs) as executor:
            futuros = [executor.submit(calcular_rota, tarefa) for tarefa in tarefas]

            concluidas = 0
            for futuro in as_completed(futuros):
                resultado = futuro.result()
                resultados.append(resultado)
                
                lote_rotas.append(resultado)
                
                if len(lote_rotas) >= tamanho_lote_rotas:
                    # Registra o momento de inserção na fila de rotas
                    momento_chegada_fila = perf_counter()
                    fila_rotas.put({
                        "timestamp": momento_chegada_fila,
                        "momento_chegada_fila_rotas": momento_chegada_fila,
                        "lote": lote_rotas
                    })
                    lote_rotas = []
                    
                concluidas += 1
                if concluidas % 500 == 0 or concluidas == len(tarefas):
                    log(f"Progresso do Roteamento: {concluidas}/{len(tarefas)}")
        if lote_rotas:
            momento_chegada_fila = perf_counter()
            fila_rotas.put({
                "timestamp": momento_chegada_fila,
                "momento_chegada_fila_rotas": momento_chegada_fila,
                "lote": lote_rotas
            })
    finally:
        workers_finalizados.set()

    # Marca o fim da fase de cálculo de rotas 
    eventos_fases["fim_roteamento"] = perf_counter()

    simulacao.join()
    processar_ficheiro_edge_data(edge_data_output, fila_db)

    db_finalizado.set()
    fila_db.put(None)
    t_db.join()

    duracao = perf_counter() - eventos_fases["inicio_total"]

    monitor.stop()
    monitor.join()

    cpu_vals, ram_vals = [], []
    while not monitor.cpu_samples.empty():
        cpu_vals.append(monitor.cpu_samples.get())
        ram_vals.append(monitor.ram_samples.get())

    avg_cpu = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0.0
    avg_ram = sum(ram_vals) / len(ram_vals) if ram_vals else 0.0
    ram_pct = psutil.virtual_memory().percent

    throughput_msg = len(tarefas) / duracao if duracao > 0 else 0.0

    latencias_valores = [lat for _, lat, _ in serie_temporal_latencias]
    total_lat = sum(latencias_valores) if latencias_valores else 0.0
    avg_lat = total_lat / len(latencias_valores) if latencias_valores else 0.0

    t_roteamento = max(0.0, eventos_fases["fim_roteamento"] - eventos_fases["inicio_total"])
    t_simulacao = max(0.0, eventos_fases["fim_simulacao"] - eventos_fases["fim_roteamento"])
    t_persistencia = max(0.0, eventos_fases["fim_persistencia"] - max(eventos_fases["fim_simulacao"], eventos_fases["fim_roteamento"]))

    prop_roteamento = (t_roteamento / duracao) * 100.0 if duracao > 0 else 0.0
    prop_simulacao = (t_simulacao / duracao) * 100.0 if duracao > 0 else 0.0
    prop_persistencia = (t_persistencia / duracao) * 100.0 if duracao > 0 else 0.0

    # Salvar série temporal em CSV detalhado incluindo a chegada nas filas
    serie_csv_path = Path(f"latencia_serie_detalhada_{os.getpid()}.csv")
    with open(serie_csv_path, mode='w', newline='', encoding='utf-8') as f_serie:
        writer_serie = csv.writer(f_serie)
        writer_serie.writerow(["tempo_decorrido_s", "latencia_lote_ms", "momento_chegada_fila_rotas_s"])
        writer_serie.writerows(serie_temporal_latencias)

    for f_path in [cfg_dinamico, add_path, edge_data_output, db_path]:
        try:
            f_path.unlink()
        except OSError:
            pass

    # Imprime os resultados formatados incluindo as métricas de tempo e proporções das fases
    print(f"RESULT:{duracao:.6f},{throughput_msg:.2f},0.0,{avg_lat:.6f},{total_lat:.6f},{avg_cpu:.2f},{avg_ram:.2f},{ram_pct:.2f},{t_roteamento:.6f},{t_simulacao:.6f},{t_persistencia:.6f},{prop_roteamento:.2f},{prop_simulacao:.2f},{prop_persistencia:.2f}", flush=True)

ENV_TRADITIONAL = os.path.join("venv_traditional", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_traditional", "bin", "python")
ENV_FREETHREADED = os.path.join("venv_free_threaded", "Scripts", "python.exe") if sys.platform == "win32" else os.path.join("venv_free_threaded", "bin", "python")

NUM_REPLICACOES = 1
TOTAL_ITEMS_STARK_SCALING = 3000

factores = {
    "python_impl": ["traditional", "free-threaded"],
    "mechanism": ["thread", "process"],
    "workers": [1, 2, 4, 8],
}

experimentos = list(itertools.product(
    factores["python_impl"],
    factores["mechanism"],
    factores["workers"]
))

def executar_replica_orquestrada(exe_target, mech, workers, items, impl):
    cmd = [
        exe_target, __file__,
        "--worker-mode",
        "--mecanismo", mech,
        "--workers", str(workers),
        "--items", str(items),
        "--impl", impl
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"Erro na execução do Worker:\n{stderr}")

    for line in stdout.splitlines():
        if line.startswith("RESULT:"):
            parts = line.replace("RESULT:", "").split(",")
            return (
                float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), 
                float(parts[4]), float(parts[5]), float(parts[6]), float(parts[7]),
                float(parts[8]), float(parts[9]), float(parts[10]), 
                float(parts[11]), float(parts[12]), float(parts[13])
            )
    raise RuntimeError("Formato de resultado inválido.")

def run_benchmark_orchestrator():
    files = {
        "free-threaded": "sumo_freethreaded_strong.csv",
        "traditional": "sumo_traditional_strong.csv"
    }

    for impl, filename in files.items():
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "implementacao", "mecanismo", "workers", "total_items", "replicacao",
                    "tempo_execucao_s", "throughput_msg_s", "throughput_mb_s", 
                    "queue_latency_media_ms", "queue_latency_total_ms",
                    "cpu_media_porcentagem", "ram_media_mb", "ram_media_porcentagem",
                    "tempo_roteamento_s", "tempo_simulacao_s", "tempo_persistencia_s",
                    "proporcao_roteamento_pct", "proporcao_simulacao_pct", "proporcao_persistencia_pct"
                ])

    total_cenarios = len(experimentos)
    for idx, (impl, mech, workers) in enumerate(experimentos, 1):
        exe_target = ENV_FREETHREADED if impl == 'free-threaded' else ENV_TRADITIONAL
        csv_target = files[impl]
        total_items = TOTAL_ITEMS_STARK_SCALING

        print("\n" + "="*70)
        print(f"[{idx}/{total_cenarios}] Strong Scaling | {impl.upper()} | Mech: {mech} | Workers: {workers} | Items: {total_items}")
        print("="*70, flush=True)

        try:
            with open(csv_target, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for rep in range(1, NUM_REPLICACOES + 1):
                    (duration, throughput_msg, throughput_mb, 
                     avg_lat, total_lat, cpu_pct, ram_mb, ram_pct,
                     t_rot, t_sim, t_per, p_rot, p_sim, p_per) = executar_replica_orquestrada(
                        exe_target, mech, workers, total_items, impl
                    )

                    writer.writerow([
                        impl, mech, workers, total_items, rep,
                        f"{duration:.6f}", f"{throughput_msg:.2f}", f"{throughput_mb:.2f}",
                        f"{avg_lat:.6f}", f"{total_lat:.6f}",
                        f"{cpu_pct:.2f}", f"{ram_mb:.2f}", f"{ram_pct:.2f}",
                        f"{t_rot:.6f}", f"{t_sim:.6f}", f"{t_per:.6f}",
                        f"{p_rot:.2f}", f"{p_sim:.2f}", f"{p_per:.2f}"
                    ])
                    f.flush()

                    print(f"  Réplica {rep}/{NUM_REPLICACOES} | Tempo: {duration:.4f}s | Roteamento: {p_rot}% | Simulação: {p_sim}% | Persistência: {p_per}%", flush=True)
        except Exception as e:
            print(f"Falha no cenário: {e}", flush=True)


if __name__ == "__main__":
    import subprocess
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-mode", action="store_true")
    parser.add_argument("--mecanismo", default="thread")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--items", type=int, default=2000)
    parser.add_argument("--impl", default="free-threaded")
    args = parser.parse_args()

    if args.worker_mode:
        executar_pipeline_worker(args.mecanismo, args.workers, args.items, args.impl)
    else:
        run_benchmark_orchestrator()