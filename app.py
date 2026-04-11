import os
import json
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
from collections import deque
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

st.set_page_config(page_title="Quire Gantt Scheduler", page_icon="📊")

# ==========================================
# 1. CONFIGURAZIONE & AUTH (QUIRE)
# ==========================================
QUIRE_CLIENT_ID       = st.secrets["QUIRE_CLIENT_ID"]
QUIRE_CLIENT_SECRET   = st.secrets["QUIRE_CLIENT_SECRET"]
INITIAL_ACCESS_TOKEN  = st.secrets["INITIAL_ACCESS_TOKEN"]
INITIAL_REFRESH_TOKEN = st.secrets["INITIAL_REFRESH_TOKEN"]

TOKEN_FILE = "quire_tokens.json"

# Setup della Sessione Resiliente
def make_resilient_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "PATCH"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session

_quire_session = make_resilient_session()

# Gestione Token
def load_quire_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return {
        "access_token":  INITIAL_ACCESS_TOKEN,
        "refresh_token": INITIAL_REFRESH_TOKEN
    }

def save_quire_tokens(access_token, refresh_token):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token, "refresh_token": refresh_token}, f)

def refresh_quire_token():
    tokens = load_quire_tokens()
    response = requests.post(
        "https://quire.io/oauth/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id":     QUIRE_CLIENT_ID,
            "client_secret": QUIRE_CLIENT_SECRET
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15
    )
    response.raise_for_status()
    new_data    = response.json()
    new_access  = new_data["access_token"]
    new_refresh = new_data.get("refresh_token", tokens["refresh_token"])
    save_quire_tokens(new_access, new_refresh)
    return new_access

def quire_api_request(method, url, json_data=None, params=None, timeout=15):
    tokens  = load_quire_tokens()
    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "Content-Type":  "application/json"
    }
    
    response = _quire_session.request(
        method, url,
        headers=headers,
        json=json_data,
        params=params,
        timeout=timeout
    )

    if response.status_code == 401:
        new_token = refresh_quire_token()
        headers["Authorization"] = f"Bearer {new_token}"
        response = _quire_session.request(
            method, url,
            headers=headers,
            json=json_data,
            params=params,
            timeout=timeout
        )

    if not response.ok:
        st.error(f"Errore API Quire: {response.status_code} - {response.text}")
        response.raise_for_status()
        
    return response.json() if response.text else {}

# ==========================================
# 2. UTILITY DATE E ID
# ==========================================
def parse_quire_date(date_str):
    if not date_str: return None
    date_str = str(date_str).strip()
    if len(date_str) == 10:
        return datetime(int(date_str[0:4]), int(date_str[5:7]), int(date_str[8:10]), tzinfo=timezone.utc)
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(date_str)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def format_quire_date(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def extract_relation_ids(field_data):
    if not field_data: return []
    if isinstance(field_data, str):
        try:
            return [str(i).replace('#', '').strip() for i in json.loads(field_data.replace("'", '"'))]
        except Exception:
            return []
    if isinstance(field_data, list):
        return [str(i).replace('#', '').strip() for i in field_data]
    return []

def get_quire_project_slug(project_identifier: str) -> str:
    url = f"https://quire.io/api/project/{project_identifier}"
    project_data = quire_api_request("GET", url)
    return project_data.get("id")

# ==========================================
# 3. MOTORE GANTT E UPDATE
# ==========================================
def _build_topo_order(tasks_map: dict) -> tuple:
    graph = {str(t_id): [] for t_id in tasks_map}
    in_degree = {str(t_id): 0 for t_id in tasks_map}
    for t_id, task in tasks_map.items():
        for succ_id in extract_relation_ids(task.get('successors')):
            if succ_id in graph:
                graph[str(t_id)].append(succ_id)
                in_degree[succ_id] += 1
    queue = deque([t for t, d in in_degree.items() if d == 0])
    topo_order = []
    visited = set()
    while queue:
        current = queue.popleft()
        if current in visited: continue
        visited.add(current)
        topo_order.append(current)
        for succ_id in graph[current]:
            in_degree[succ_id] -= 1
            if in_degree[succ_id] == 0:
                queue.append(succ_id)
    return topo_order, graph

def _gantt_apply_update(task_oid: str, task: dict, direction: str = "forward"):
    due_date   = task.get('due', '')
    start_date = task.get('start')

    def clean_dt(d):
        if not d: return None
        if 'T' in d: return d[:16] + 'Z'
        return d

    due_clean = clean_dt(due_date)
    start_clean = clean_dt(start_date)

    if direction == "backward":
        if start_clean:
            quire_api_request("PUT", f"https://quire.io/api/task/{task_oid}", json_data={"start": start_clean})
        return quire_api_request("PUT", f"https://quire.io/api/task/{task_oid}", json_data={"due": due_clean})
    else:
        res = quire_api_request("PUT", f"https://quire.io/api/task/{task_oid}", json_data={"due": due_clean})
        if start_clean:
            quire_api_request("PUT", f"https://quire.io/api/task/{task_oid}", json_data={"start": start_clean})
        return res

def project_push_forward(tasks_map: dict) -> list:
    total_modified = []
    topo_order, graph = _build_topo_order(tasks_map)

    for t_id in topo_order:
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'): continue
        current_due = parse_quire_date(current_task['due'])

        for succ_id in graph[t_id]:
            succ_task = tasks_map.get(succ_id)
            if not succ_task or not succ_task.get('due'): continue

            succ_due   = parse_quire_date(succ_task['due'])
            has_start  = bool(succ_task.get('start'))
            succ_start = parse_quire_date(succ_task['start']) if has_start else succ_due

            if succ_start <= current_due:
                shift_delta = (current_due + timedelta(days=1)) - succ_start
                new_start = succ_start + shift_delta
                new_due   = succ_due + shift_delta

                if has_start:
                    succ_task['start'] = format_quire_date(new_start)
                succ_task['due'] = format_quire_date(new_due)
                total_modified.append(succ_task)

    return total_modified

def project_push_backward(tasks_map: dict) -> list:
    total_modified = []
    topo_order, graph = _build_topo_order(tasks_map)

    pred_graph = {str(t_id): [] for t_id in tasks_map}
    for t_id, succs in graph.items():
        for succ_id in succs:
            pred_graph[succ_id].append(t_id)

    for t_id in reversed(topo_order):
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'): continue

        current_start_explicit = bool(current_task.get('start'))
        current_start = (
            parse_quire_date(current_task['start'])
            if current_start_explicit
            else parse_quire_date(current_task['due'])
        )

        for pred_id in pred_graph[t_id]:
            pred_task = tasks_map.get(pred_id)
            if not pred_task or not pred_task.get('due'): continue

            pred_due   = parse_quire_date(pred_task['due'])
            has_start  = bool(pred_task.get('start'))
            pred_start = parse_quire_date(pred_task['start']) if has_start else pred_due

            conflict = (pred_due >= current_start) if current_start_explicit else (pred_due > current_start)
            #conflict = (pred_due > current_start)
            conflict = (pred_due >= current_start)

            if conflict:
                shift_delta = pred_due - (current_start - timedelta(days=1))
                new_due   = pred_due - shift_delta
                new_start = pred_start - shift_delta

                if has_start:
                    pred_task['start'] = format_quire_date(new_start)
                pred_task['due'] = format_quire_date(new_due)
                total_modified.append(pred_task)

    return total_modified

# ==========================================
# 4. FRONTEND STREAMLIT
# ==========================================


# ==========================================
# 4. FRONTEND STREAMLIT ("One-Click" Live)
# ==========================================
st.title("📊 Quire Gantt Scheduler")
st.markdown("Forza lo slittamento dei task in base alle dipendenze per non sovrapporli.")

st.write("---")
# Progetto di default (il tuo ID corto o OID)
DEFAULT_PROJECT = "0PfverYTSAVEXRuTLyDdnLuI" 

col1, col2 = st.columns(2)
with col1:
    input_oid = st.text_input("Inserisci Project OID", placeholder="Lascia vuoto per usare il default")
with col2:
    input_id = st.text_input("Oppure Project ID (es. DB_MLO4)", placeholder="Lascia vuoto per usare il default")

# Se l'utente compila qualcosa lo usiamo, altrimenti usiamo il default
target_project = input_oid if input_oid else (input_id if input_id else DEFAULT_PROJECT)

st.write(f"**Progetto target attuale:** `{target_project}`")
st.write("---")

col3, col4 = st.columns(2)

# --- BOTTONE FORWARD ---
with col3:
    if st.button("⏩ Push Gantt FORWARD", type="primary", use_container_width=True):
        with st.spinner("1/3 Sincronizzazione task live da Quire..."):
            try:
                slug = get_quire_project_slug(target_project)
                tasks = quire_api_request("GET", f"https://quire.io/api/task/search/id/{slug}", params={"scheduled": "true", "limit": "no"})
                tasks_map = {str(t['id']): t for t in tasks}
            except Exception as e:
                st.error(f"Errore nel fetch dei task: {e}")
                st.stop()
                
        with st.spinner("2/3 Calcolo conflitti in corso..."):
            modificati = project_push_forward(tasks_map)
            
        with st.spinner(f"3/3 Applicazione di {len(modificati)} aggiornamenti..."):
            for task in modificati:
                _gantt_apply_update(task['oid'], task, direction="forward")
                
        st.success(f"✅ Push Forward completato! {len(modificati)} task slittati in avanti.")
        if modificati:
            st.json([{"Task": t['name'], "Nuovo Start": t.get('start'), "Nuova Scadenza": t.get('due')} for t in modificati])

# --- BOTTONE BACKWARD ---
with col4:
    if st.button("⏪ Push Gantt BACKWARD", type="primary", use_container_width=True):
        with st.spinner("1/3 Sincronizzazione task live da Quire..."):
            try:
                slug = get_quire_project_slug(target_project)
                tasks = quire_api_request("GET", f"https://quire.io/api/task/search/id/{slug}", params={"scheduled": "true", "limit": "no"})
                tasks_map = {str(t['id']): t for t in tasks}
            except Exception as e:
                st.error(f"Errore nel fetch dei task: {e}")
                st.stop()

        with st.spinner("2/3 Calcolo conflitti in corso..."):
            modificati = project_push_backward(tasks_map)
            
        with st.spinner(f"3/3 Applicazione di {len(modificati)} aggiornamenti..."):
            for task in modificati:
                _gantt_apply_update(task['oid'], task, direction="backward")
                
        st.success(f"✅ Push Backward completato! {len(modificati)} task slittati all'indietro.")
        if modificati:
            st.json([{"Task": t['name'], "Nuovo Start": t.get('start'), "Nuova Scadenza": t.get('due')} for t in modificati])




# ==========================================
# 5. CREAZIONE CATENA DI DIPENDENZE (Versione Filtrata)
# ==========================================
st.write("---")
st.header("🔗 Crea Catena di Dipendenze")
st.markdown("Assegna lo stato `to_be_linked` ai task in Quire. Poi usa questo tool per collegarli in serie. A operazione completata torneranno 'Da fare'.")

if "chain_df" not in st.session_state:
    st.session_state.chain_df = None
if "chain_tasks_map" not in st.session_state:
    st.session_state.chain_tasks_map = {}

# Sostituisci questo valore con il nome esatto dello stato "normale" a cui devono tornare 
# i task dopo essere stati incatenati (o usa il valore numerico, di default 0 è 'Da fare')
TARGET_STATUS_NAME = "to_be_linked"
RESET_STATUS_VALUE = 0  # Valore di default per rimetterli "in coda/da fare"

if st.button("🔍 1. Cerca Task da Incatenare", use_container_width=True):
    with st.spinner("Cerco i task con stato 'to_be_linked'..."):
        try:
            slug = get_quire_project_slug(target_project)
            
            # 1. Chiamiamo l'API chiedendo SOLO i task attivi, aumentando il timeout
            tasks = quire_api_request(
                "GET", 
                f"https://quire.io/api/task/search/id/{slug}", 
                params={"status": "active", "limit": "no"},
                timeout=25 # Aumentato per evitare il read timeout
            )
            
            chain_tasks = []
            st.session_state.chain_tasks_map = {}
            
            # 2. Filtriamo quelli che hanno il nostro stato personalizzato
            for t in tasks:
                status_obj = t.get('status', {})
                status_name = status_obj.get('name', '').lower()
                
                # Se il nome dello stato coincide (case-insensitive)
                if status_name == TARGET_STATUS_NAME.lower():
                    chain_tasks.append(t)
                    st.session_state.chain_tasks_map[t['oid']] = t
            
            if not chain_tasks:
                st.warning(f"Nessun task trovato con stato '{TARGET_STATUS_NAME}'.")
                st.session_state.chain_df = None
            else:
                chain_tasks.sort(key=lambda x: int(x['id']))
                
                df = pd.DataFrame({
                    "Ordine": range(1, len(chain_tasks) + 1),
                    "ID": [t['id'] for t in chain_tasks],
                    "Nome": [t['name'] for t in chain_tasks],
                    "OID": [t['oid'] for t in chain_tasks]
                })
                st.session_state.chain_df = df
                
        except Exception as e:
            st.error(f"Errore durante la ricerca: {e}")

if st.session_state.chain_df is not None:
    st.write("**2. Modifica i numeri nella colonna 'Ordine' se vuoi cambiare la sequenza:**")
    
    edited_df = st.data_editor(
        st.session_state.chain_df, 
        disabled=["ID", "Nome", "OID"],
        hide_index=True,
        use_container_width=True
    )
    
    if st.button("🔗 3. Applica Catena e Ripristina Stato", type="primary", use_container_width=True):
        with st.spinner("Creazione dipendenze in corso..."):
            sorted_df = edited_df.sort_values("Ordine").reset_index(drop=True)
            ordered_oids = sorted_df["OID"].tolist()
            ordered_ids = sorted_df["ID"].tolist()
            
            success_count = 0
            
            for i in range(len(ordered_oids)):
                current_oid = ordered_oids[i]
                current_task = st.session_state.chain_tasks_map[current_oid]
                
                # --- RIPRISTINO STATO ---
                payload = {"status": RESET_STATUS_VALUE}
                
                # --- GESTIONE SUCCESSORI ---
                if i < len(ordered_oids) - 1:
                    next_id = ordered_ids[i+1]
                    existing_succs = extract_relation_ids(current_task.get('successors'))
                    new_succs = list(set(existing_succs + [str(next_id)]))
                    payload["successors"] = new_succs
                
                try:
                    quire_api_request("PUT", f"https://quire.io/api/task/{current_oid}", json_data=payload)
                    success_count += 1
                except Exception as e:
                    st.error(f"Errore nell'aggiornamento del task {ordered_ids[i]}: {e}")
            
            if success_count == len(ordered_oids):
                st.success(f"✅ Catena creata perfettamente! I task sono stati rimescolati e rimessi in coda.")
                st.session_state.chain_df = None

