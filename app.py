import os
import json
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
from collections import deque
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
st.title("📊 Quire Gantt Scheduler")
st.markdown("Forza lo slittamento dei task in base alle dipendenze per non sovrapporli.")

st.write("---")
col1, col2 = st.columns(2)
with col1:
    input_oid = st.text_input("Inserisci Project OID (Opzionale)", placeholder="es. 0PfverYTSAVEXRu...")
with col2:
    input_id = st.text_input("Oppure inserisci Project ID", placeholder="es. DB_MLO4")

target_project = input_oid if input_oid else input_id

if st.button("Carica Dati Progetto"):
    if not target_project:
        st.warning("Inserisci un OID o un ID progetto.")
    else:
        with st.spinner("Risoluzione ID e download task in corso..."):
            slug = get_quire_project_slug(target_project)
            # Fetch usando lo slug per maggiore affidabilità
            tasks = quire_api_request("GET", f"https://quire.io/api/task/search/id/{slug}", params={"scheduled": "true", "limit": "no"})
            st.session_state['tasks_map'] = {str(t['id']): t for t in tasks}
            st.success(f"Caricati {len(tasks)} task schedulati con successo!")

st.write("---")

col3, col4 = st.columns(2)
with col3:
    if st.button("⏩ Push Gantt FORWARD", type="primary"):
        if 'tasks_map' not in st.session_state:
            st.error("Carica prima i dati del progetto!")
        else:
            with st.spinner("Calcolo e applicazione Push Forward in corso..."):
                modificati = project_push_forward(st.session_state['tasks_map'])
                for task in modificati:
                    _gantt_apply_update(task['oid'], task, direction="forward")
                st.success(f"Push Forward completato! {len(modificati)} task aggiornati su Quire.")
                if modificati:
                    st.json([{"Task": t['name'], "Nuovo Start": t.get('start'), "Nuova Scadenza": t.get('due')} for t in modificati])

with col4:
    if st.button("⏪ Push Gantt BACKWARD", type="primary"):
        if 'tasks_map' not in st.session_state:
            st.error("Carica prima i dati del progetto!")
        else:
            with st.spinner("Calcolo e applicazione Push Backward in corso..."):
                modificati = project_push_backward(st.session_state['tasks_map'])
                for task in modificati:
                    _gantt_apply_update(task['oid'], task, direction="backward")
                st.success(f"Push Backward completato! {len(modificati)} task aggiornati su Quire.")
                if modificati:
                    st.json([{"Task": t['name'], "Nuovo Start": t.get('start'), "Nuova Scadenza": t.get('due')} for t in modificati])
