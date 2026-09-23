from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

R_BANDA = 1.0     # 1 char/s
D_PROC = 0.2      # Atraso de Processamento (s)
D_PROP = 0.3      # Atraso de Propagação (s)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/simular_universal', methods=['POST'])
def simular_universal():
    data = request.get_json()
    routers = data.get('routers', ['R1', 'R2'])
    host_router_map = data.get('host_router_map', {})
    nodes = list(host_router_map.keys())
    transmissions = data.get('transmissions', [])
    buffer_limit = int(data.get('buffer_limit', 4))
    l_chars = int(data.get('l_chars', 3))
    protocol = data.get('protocol', 'TCP')

    d_trans = round(l_chars / R_BANDA, 2)
    fragmentacao_info = []
    pacotes_dados = []

    # 1. ESTÁGIO DE FRAGMENTAÇÃO
    for tx in transmissions:
        src, dst, msg = tx.get('src'), tx.get('dst'), tx.get('msg', '').strip()
        if msg and src in nodes and dst in nodes and src != dst:
            fatias = [msg[i:i + l_chars] for i in range(0, len(msg), l_chars)]
            pkts_tx = []
            for idx, payload in enumerate(fatias, start=1):
                pkt = {
                    'id': f"{src}->{dst}-P{idx}",
                    'src': src,
                    'dst': dst,
                    'seq': idx,
                    'payload': payload,
                    'r_origem': host_router_map.get(src),
                    'r_destino': host_router_map.get(dst)
                }
                pkts_tx.append(pkt)
                pacotes_dados.append(pkt)

            fragmentacao_info.append({
                'src': src,
                'dst': dst,
                'msg_original': msg,
                'total_pacotes': len(fatias),
                'pacotes': pkts_tx
            })

    eventos = [{
        'tipo': 'fragmentacao_concluida',
        'fragmentacao': fragmentacao_info,
        'd_trans': d_trans,
        'd_proc': D_PROC,
        'd_prop': D_PROP,
        'buffer_limit': buffer_limit,
        'protocol': protocol,
        'log': f"🧩 [FRAGMENTAÇÃO DINÂMICA] {len(pacotes_dados)} pacotes gerados em uma rede com {len(routers)} Roteadores e {len(nodes)} Hosts."
    }]

    buffers = {r: [] for r in routers}
    caixas_entrada = {u: [] for u in nodes}

    # 2. ENTRADA NOS ROTEADORES DE ORIGEM (HOST -> R_SRC)
    for pkt in pacotes_dados:
        r_src = pkt['r_origem']
        payload = pkt['payload']
        src = pkt['src']

        if not r_src or r_src not in buffers:
            continue

        if len(buffers[r_src]) < buffer_limit:
            buffers[r_src].append(pkt)
            d_fila = round(len(buffers[r_src]) * d_trans, 2)
            d_nodal = round(D_PROC + D_PROP + d_trans + d_fila, 2)

            eventos.append({
                'tipo': 'chegada_roteador',
                'roteador': r_src,
                'pacote': pkt,
                'sucesso': True,
                'buffers': {r: len(buffers[r]) for r in routers},
                'delays': {'d_trans': d_trans, 'd_proc': D_PROC, 'd_prop': D_PROP, 'd_fila': d_fila, 'd_nodal': d_nodal},
                'log': f"📦 [{src} ➔ {r_src}] Pacote P{pkt['seq']} ('{payload}') entrou na fila do roteador {r_src}."
            })
        else:
            d_fila = round(buffer_limit * d_trans, 2)
            d_nodal = round(D_PROC + D_PROP + d_trans + d_fila, 2)
            eventos.append({
                'tipo': 'chegada_roteador',
                'roteador': r_src,
                'pacote': pkt,
                'sucesso': False,
                'buffers': {r: len(buffers[r]) for r in routers},
                'delays': {'d_trans': d_trans, 'd_proc': D_PROC, 'd_prop': D_PROP, 'd_fila': d_fila, 'd_nodal': d_nodal},
                'log': f"💥 [OVERFLOW {r_src}] Fila cheia em {r_src}! Pacote '{payload}' descartado."
            })

    # 3. REPASSE INTER-ROTEADOR E ENTREGA FINAL
    for r_nome in list(routers):
        while len(buffers[r_nome]) > 0:
            pkt_proc = buffers[r_nome].pop(0)
            payload = pkt_proc['payload']
            r_dst = pkt_proc['r_destino']

            # Se o destino pertence a outro roteador
            if r_nome != r_dst:
                if r_dst in buffers and len(buffers[r_dst]) < buffer_limit:
                    buffers[r_dst].append(pkt_proc)
                    eventos.append({
                        'tipo': 'salto_inter_roteador',
                        'r_origem': r_nome,
                        'r_destino': r_dst,
                        'pacote': pkt_proc,
                        'buffers': {r: len(buffers[r]) for r in routers},
                        'log': f"🔗 [SALTO ROTEADOR] Pacote '{payload}' repassado de {r_nome} ➔ {r_dst}."
                    })
                else:
                    eventos.append({
                        'tipo': 'drop_inter_roteador',
                        'r_origem': r_nome,
                        'r_destino': r_dst,
                        'pacote': pkt_proc,
                        'buffers': {r: len(buffers[r]) for r in routers},
                        'log': f"💥 [DROP ENLACE] Buffer de {r_dst} lotado! Pacote '{payload}' perdido no trajeto {r_nome} ➔ {r_dst}."
                    })
            else:
                # Entrega no host final
                target = pkt_proc['dst']
                caixas_entrada[target].append(pkt_proc)
                eventos.append({
                    'tipo': 'entrega_destino',
                    'roteador': r_nome,
                    'pacote': pkt_proc,
                    'buffers': {r: len(buffers[r]) for r in routers},
                    'log': f"📬 [{r_nome} ➔ {target}] Pacote P{pkt_proc['seq']} ('{payload}') entregue com sucesso."
                })

                if protocol == 'TCP':
                    eventos.append({
                        'tipo': 'entrega_ack',
                        'src': target,
                        'dst': pkt_proc['src'],
                        'pacote': {'payload': f"ACK_{pkt_proc['seq']}"},
                        'buffers': {r: len(buffers[r]) for r in routers},
                        'log': f"❇️ [ACK TCP] {target} enviou confirmação ACK_{pkt_proc['seq']} para {pkt_proc['src']}."
                    })

    # Reconstituição final
    mensagens_estruturadas = {}
    for user, pkts in caixas_entrada.items():
        agrupado = {}
        for p in pkts:
            rem = p['src']
            if rem not in agrupado: agrupado[rem] = []
            agrupado[rem].append(p['payload'])
        mensagens_estruturadas[user] = [{'de': rem, 'conteudo': "".join(fatias)} for rem, fatias in agrupado.items()]

    eventos.append({
        'tipo': 'fim',
        'mensagens': mensagens_estruturadas,
        'log': "🏁 Simulação da Topologia Dinâmica Concluída!"
    })

    return jsonify(eventos)

if __name__ == '__main__':
    app.run(debug=True, port=5000)