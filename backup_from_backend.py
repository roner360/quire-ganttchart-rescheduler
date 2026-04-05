

import math

def _normalize_tasks_map(tasks_map: dict) -> dict:
    """Normalizza start e due di tutti i task a YYYY-MM-DD prima di qualsiasi calcolo."""
    for task in tasks_map.values():
        if task.get('due'):
            dt = parse_quire_date(task['due'])
            if dt:
                task['due'] = format_quire_date(dt)
        if task.get('start'):
            dt = parse_quire_date(task['start'])
            if dt:
                task['start'] = format_quire_date(dt)
    return tasks_map


def _get_task_duration_days(task: dict) -> int:
    """
    Durata in giorni interi.
    Presuppone start/due già normalizzati da _normalize_tasks_map.
    0 = task puntuale (start == due), preservato.
    """
    due   = parse_quire_date(task.get('due'))
    start = parse_quire_date(task.get('start')) if task.get('start') else None

    if due and start:
        days = (due - start).days
        if days >= 0:
            return days

    etc = task.get('etc')
    if etc and isinstance(etc, (int, float)) and etc > 0:
        return max(1, math.ceil(etc / 86400))

    return 0  # fallback: task puntuale, non inventiamo durata


def project_push_forward(tasks_map: dict) -> set:
    tasks_map         = _normalize_tasks_map(tasks_map)
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    for t_id in topo_order:
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue
        # Rileggi sempre dal tasks_map aggiornato
        current_due = parse_quire_date(tasks_map[t_id]['due'])

        for succ_id in graph[t_id]:
            succ_task = tasks_map.get(succ_id)
            if not succ_task or not succ_task.get('due'):
                continue

            succ_due      = parse_quire_date(succ_task['due'])
            has_start     = bool(succ_task.get('start'))
            succ_start    = parse_quire_date(succ_task['start']) if has_start else succ_due
            duration_days = _get_task_duration_days(succ_task)

            if succ_start <= current_due:
                new_start = current_due + timedelta(days=1)
                new_due   = new_start + timedelta(days=duration_days)

                QM_LOG.info(
                    f"[PushForward] '{current_task.get('name')}' due={format_quire_date(current_due)} | "
                    f"'{succ_task.get('name')}' "
                    f"start {format_quire_date(succ_start)}→{format_quire_date(new_start)} | "
                    f"due {format_quire_date(succ_due)}→{format_quire_date(new_due)} | "
                    f"durata={duration_days}gg"
                )

                if has_start:
                    succ_task['start'] = format_quire_date(new_start)
                succ_task['due'] = format_quire_date(new_due)
                total_modified.add(succ_id)

    QM_LOG.info(f"[PushForward] Completato. Task modificati: {len(total_modified)}")
    return total_modified


def project_push_backward(tasks_map: dict) -> set:
    tasks_map         = _normalize_tasks_map(tasks_map)
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    pred_graph = {str(t_id): [] for t_id in tasks_map}
    for t_id, succs in graph.items():
        for succ_id in succs:
            pred_graph[succ_id].append(t_id)

    for t_id in reversed(topo_order):
        # Rileggi sempre dal tasks_map aggiornato — il task potrebbe
        # essere già stato modificato da un'iterazione precedente
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue

        current_start_explicit = bool(current_task.get('start'))
        # Rileggi start/due freschi dal dizionario (già aggiornato)
        current_start = (
            parse_quire_date(tasks_map[t_id]['start'])
            if current_start_explicit
            else parse_quire_date(tasks_map[t_id]['due'])
        )

        for pred_id in pred_graph[t_id]:
            pred_task = tasks_map.get(pred_id)
            if not pred_task or not pred_task.get('due'):
                continue

            pred_due      = parse_quire_date(pred_task['due'])
            has_start     = bool(pred_task.get('start'))
            pred_start    = parse_quire_date(pred_task['start']) if has_start else pred_due
            duration_days = _get_task_duration_days(pred_task)

            conflict = (
                (pred_due >= current_start)
                if current_start_explicit
                else (pred_due > current_start)
            )

            if conflict:
                new_due   = current_start - timedelta(days=1)
                new_start = new_due - timedelta(days=duration_days)

                QM_LOG.info(
                    f"[PushBackward] '{current_task.get('name')}' "
                    f"start={'espl' if current_start_explicit else 'ass'}="
                    f"{format_quire_date(current_start)} | "
                    f"'{pred_task.get('name')}' "
                    f"due {format_quire_date(pred_due)}→{format_quire_date(new_due)} | "
                    f"start {format_quire_date(pred_start)}→{format_quire_date(new_start)} | "
                    f"durata={duration_days}gg"
                )

                if has_start:
                    pred_task['start'] = format_quire_date(new_start)
                pred_task['due'] = format_quire_date(new_due)
                total_modified.add(pred_id)

    QM_LOG.info(f"[PushBackward] Completato. Task modificati: {len(total_modified)}")
    return total_modified
def project_push_forward(tasks_map: dict) -> set:
    tasks_map         = _normalize_tasks_map(tasks_map)
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    for t_id in topo_order:
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue
        
        # Rileggi sempre dal tasks_map aggiornato
        current_due = parse_quire_date(tasks_map[t_id]['due'])

        for succ_id in graph[t_id]:
            succ_task = tasks_map.get(succ_id)
            if not succ_task or not succ_task.get('due'):
                continue

            succ_due   = parse_quire_date(succ_task['due'])
            has_start  = bool(succ_task.get('start'))
            succ_start = parse_quire_date(succ_task['start']) if has_start else succ_due

            if succ_start <= current_due:
                # Calcoliamo lo SLITTAMENTO ESATTO (Delta) necessario per risolvere il conflitto
                shift_delta = (current_due + timedelta(days=1)) - succ_start
                
                # Trasliamo l'intero task applicando lo stesso Delta a start e due.
                # Questo preserva la durata originale al millisecondo.
                new_start = succ_start + shift_delta
                new_due   = succ_due + shift_delta

                QM_LOG.info(
                    f"[PushForward] '{current_task.get('name')}' due={format_quire_date(current_due)} | "
                    f"'{succ_task.get('name')}' "
                    f"start {format_quire_date(succ_start)}→{format_quire_date(new_start)} | "
                    f"due {format_quire_date(succ_due)}→{format_quire_date(new_due)}"
                )

                if has_start:
                    succ_task['start'] = format_quire_date(new_start)
                succ_task['due'] = format_quire_date(new_due)
                total_modified.add(succ_id)

    QM_LOG.info(f"[PushForward] Completato. Task modificati: {len(total_modified)}")
    return total_modified


def project_push_backward(tasks_map: dict) -> set:
    tasks_map         = _normalize_tasks_map(tasks_map)
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    pred_graph = {str(t_id): [] for t_id in tasks_map}
    for t_id, succs in graph.items():
        for succ_id in succs:
            pred_graph[succ_id].append(t_id)

    for t_id in reversed(topo_order):
        # Rileggi sempre dal tasks_map aggiornato
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue

        current_start_explicit = bool(current_task.get('start'))
        current_start = (
            parse_quire_date(tasks_map[t_id]['start'])
            if current_start_explicit
            else parse_quire_date(tasks_map[t_id]['due'])
        )

        for pred_id in pred_graph[t_id]:
            pred_task = tasks_map.get(pred_id)
            if not pred_task or not pred_task.get('due'):
                continue

            pred_due   = parse_quire_date(pred_task['due'])
            has_start  = bool(pred_task.get('start'))
            pred_start = parse_quire_date(pred_task['start']) if has_start else pred_due

            conflict = (
                (pred_due >= current_start)
                if current_start_explicit
                else (pred_due > current_start)
            )

            if conflict:
                # Calcoliamo lo SLITTAMENTO ALL'INDIETRO ESATTO (Delta)
                shift_delta = pred_due - (current_start - timedelta(days=1))
                
                # Trasliamo l'intero task sottraendo lo stesso Delta
                new_due   = pred_due - shift_delta
                new_start = pred_start - shift_delta

                QM_LOG.info(
                    f"[PushBackward] '{current_task.get('name')}' "
                    f"start={'espl' if current_start_explicit else 'ass'}={format_quire_date(current_start)} | "
                    f"'{pred_task.get('name')}' "
                    f"due {format_quire_date(pred_due)}→{format_quire_date(new_due)} | "
                    f"start {format_quire_date(pred_start)}→{format_quire_date(new_start)}"
                )

                if has_start:
                    pred_task['start'] = format_quire_date(new_start)
                pred_task['due'] = format_quire_date(new_due)
                total_modified.add(pred_id)

    QM_LOG.info(f"[PushBackward] Completato. Task modificati: {len(total_modified)}")
    return total_modified


def project_push_forward(tasks_map: dict) -> set:
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    for t_id in topo_order:
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue
        
        current_due = parse_quire_date(current_task['due'])

        for succ_id in graph[t_id]:
            succ_task = tasks_map.get(succ_id)
            if not succ_task or not succ_task.get('due'):
                continue

            succ_due   = parse_quire_date(succ_task['due'])
            has_start  = bool(succ_task.get('start'))
            succ_start = parse_quire_date(succ_task['start']) if has_start else succ_due

            if succ_start <= current_due:
                # 1. Calcolo del Delta: di quanto deve slittare in avanti?
                shift_delta = (current_due + timedelta(days=1)) - succ_start
                
                # 2. Traslazione rigida: la durata non si tocca!
                new_start = succ_start + shift_delta
                new_due   = succ_due + shift_delta

                QM_LOG.info(
                    f"[PushForward] '{current_task.get('name')}' due={format_quire_date(current_due)} | "
                    f"'{succ_task.get('name')}' "
                    f"start {format_quire_date(succ_start)}→{format_quire_date(new_start)} | "
                    f"due {format_quire_date(succ_due)}→{format_quire_date(new_due)}"
                )

                if has_start:
                    succ_task['start'] = format_quire_date(new_start)
                succ_task['due'] = format_quire_date(new_due)
                total_modified.add(succ_id)

    QM_LOG.info(f"[PushForward] Task modificati: {len(total_modified)}")
    return total_modified


def project_push_backward(tasks_map: dict) -> set:
    total_modified    = set()
    topo_order, graph = _build_topo_order(tasks_map)

    pred_graph = {str(t_id): [] for t_id in tasks_map}
    for t_id, succs in graph.items():
        for succ_id in succs:
            pred_graph[succ_id].append(t_id)

    for t_id in reversed(topo_order):
        current_task = tasks_map.get(t_id)
        if not current_task or not current_task.get('due'):
            continue

        current_start_explicit = bool(current_task.get('start'))
        current_start = (
            parse_quire_date(current_task['start'])
            if current_start_explicit
            else parse_quire_date(current_task['due'])
        )

        for pred_id in pred_graph[t_id]:
            pred_task = tasks_map.get(pred_id)
            if not pred_task or not pred_task.get('due'):
                continue

            pred_due   = parse_quire_date(pred_task['due'])
            has_start  = bool(pred_task.get('start'))
            pred_start = parse_quire_date(pred_task['start']) if has_start else pred_due

            conflict = (
                (pred_due >= current_start)
                if current_start_explicit
                else (pred_due > current_start)
            )

            if conflict:
                # 1. Calcolo del Delta: di quanto deve slittare all'indietro?
                shift_delta = pred_due - (current_start - timedelta(days=1))
                
                # 2. Traslazione rigida: la durata non si tocca!
                new_due   = pred_due - shift_delta
                new_start = pred_start - shift_delta

                QM_LOG.info(
                    f"[PushBackward] '{current_task.get('name')}' "
                    f"start={'espl' if current_start_explicit else 'ass'}={format_quire_date(current_start)} | "
                    f"'{pred_task.get('name')}' "
                    f"due {format_quire_date(pred_due)}→{format_quire_date(new_due)} | "
                    f"start {format_quire_date(pred_start)}→{format_quire_date(new_start)}"
                )

                if has_start:
                    pred_task['start'] = format_quire_date(new_start)
                pred_task['due'] = format_quire_date(new_due)
                total_modified.add(pred_id)

    QM_LOG.info(f"[PushBackward] Task modificati: {len(total_modified)}")
    return total_modified


def qm_update_quire_task(q_oid, task_data):
    """
    Aggiorna un task Quire. q_oid deve essere sempre un OID lungo.
    task_data può venire da Motion o dal Gantt engine.
    """
    task_name = task_data.get('name', 'Unknown')

    priority_raw = task_data.get('priority')
    if isinstance(priority_raw, dict):
        priority_val = priority_raw.get('value', 0)
    else:
        priority_val = motion_priority_to_quire(priority_raw)

    due_date = task_data.get('due') or task_data.get('dueDate')
    if due_date and 'T' in due_date:
        due_date = due_date[:16] + 'Z'

    # INIEZIONE CHIRURGICA: Recuperiamo anche lo start!
    start_date = task_data.get('start')
    if start_date and 'T' in start_date:
        start_date = start_date[:16] + 'Z'

    # Mappatura status
    quire_status = motion_completed_to_quire_status(task_data)

    data = {
        "name":     task_name,
        "due":      due_date,
        "start":    start_date,  # ORA LO SPEDIAMO A QUIRE!
        "priority": priority_val,
        "status":   quire_status,
    }
    
    if task_data.get('description'):
        data["description"] = task_data['description']
        
    etc = task_data.get('etc') or motion_duration_to_quire_etc(task_data.get('duration'))
    if etc is not None:
        data["etc"] = etc

    # Pulizia: togliamo i valori None per non far arrabbiare Quire
    data = {k: v for k, v in data.items() if v is not None}

    try:
        QM_LOG.info(f"==> [QUIRE] Update OID {q_oid} | start={start_date} due={due_date}")
        resp = quire_api_request("PUT", f"https://quire.io/api/task/{q_oid}", json_data=data)
        QM_LOG.info(f"==> [QUIRE] ✅ Update riuscito per '{task_name}'")
        return resp
    except Exception as e:
        QM_LOG.error(f"==> [QUIRE] ❌ ERRORE update OID {q_oid} | payload: {data} | errore: {e}")
        raise
