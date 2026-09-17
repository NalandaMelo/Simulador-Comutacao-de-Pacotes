from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

R_BANDA = 1.0     # Taxa de transmissão (1 char/seg)
D_PROC = 0.2      # Atraso de Processamento (s)
D_PROP = 0.3      # Atraso de Propagação (s)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/simular_universal', methods=['POST'])
def simular_universal():
    data = request.get_json()
    nodes = data.get('nodes', [])
    transmissions = data.get('transmissions', [])
    buffer_limit = int(data.get('buffer_limit', 4))
    l_chars = int(data.get('l_chars', 3))
    protocol = data.get('protocol', 'TCP')

    # 1. FRAGMENTAÇÃO
    fragmentacao_info = []
    pacotes_dados = []

    for tx in transmissions:
        src, dst, msg = tx.get('src'), tx.get('dst'), tx.get('msg', '').strip()
        if msg and src in nodes and dst in nodes and src != dst:
            fatias = [msg[i:i + l_chars] for i in range(0, len(msg), l_chars)]
            pkts_tx = []
            for idx, payload in enumerate(fatias, start=1):
                pkt = {
                    'id': f"{src}->{dst}-P{idx}",
                    'tipo': 'DATA',
                    'src': src,
                    'dst': dst,
                    'seq': idx,
                    'total_seq': len(fatias),
                    'payload': payload
                }
                pacotes_dados.append(pkt)
                pkts_tx.append(pkt)

            fragmentacao_info.append({
                'src': src,
                'dst': dst,
                'msg_original': msg,
                'total_pacotes': len(fatias),
                'pacotes': pkts_tx
            })

    d_trans = round(l_chars / R_BANDA, 2)
    eventos = []

    eventos.append({
        'tipo': 'fragmentacao_concluida',
        'fragmentacao': fragmentacao_info,
        'd_trans': d_trans,
        'd_proc': D_PROC,
        'd_prop': D_PROP,
        'buffer_limit': buffer_limit,
        'protocol': protocol,
        'log': f"🧩 [FRAGMENTAÇÃO] Protocolo {protocol} selecionado. {len(pacotes_dados)} pacotes gerados."
    })

    buffer_roteador = []
    caixas_entrada = {u: [] for u in nodes}

    # 2. PROCESSAMENTO COM LÓGICA UDP / TCP
    for pkt in pacotes_dados:
        entregue = False
        tentativas = 0

        while not entregue:
            src, dst, payload = pkt['src'], pkt['dst'], pkt['payload']
            tentativas += 1

            if len(buffer_roteador) < buffer_limit:
                buffer_roteador.append(pkt)
                entregue = True
                d_fila = round(len(buffer_roteador) * d_trans, 2)
                d_nodal = round(D_PROC + D_PROP + d_trans + d_fila, 2)

                tag = "🔄 [TCP RETRANSMISSÃO]" if tentativas > 1 else "🟢 [CHEGADA]"
                eventos.append({
                    'fase': 'IDA',
                    'tipo': 'chegada_roteador',
                    'pacote': pkt,
                    'sucesso': True,
                    'buffer_atual': len(buffer_roteador),
                    'delays': {'d_trans': d_trans, 'd_proc': D_PROC, 'd_prop': D_PROP, 'd_fila': d_fila, 'd_nodal': d_nodal},
                    'log': f"{tag} Pacote '{payload}' ({src}➔{dst}) na fila. Buffer: {len(buffer_roteador)}/{buffer_limit}"
                })
            else:
                d_fila = round(buffer_limit * d_trans, 2)
                d_nodal = round(D_PROC + D_PROP + d_trans + d_fila, 2)

                if protocol == "UDP":
                    eventos.append({
                        'fase': 'IDA',
                        'tipo': 'chegada_roteador',
                        'pacote': pkt,
                        'sucesso': False,
                        'buffer_atual': len(buffer_roteador),
                        'delays': {'d_trans': d_trans, 'd_proc': D_PROC, 'd_prop': D_PROP, 'd_fila': d_fila, 'd_nodal': d_nodal},
                        'log': f"🔴 [UDP OVERFLOW] Buffer cheio! Pacote '{payload}' de {src} DESCARTADO sem retransmissão."
                    })
                    entregue = True
                else:
                    eventos.append({
                        'fase': 'IDA',
                        'tipo': 'chegada_roteador',
                        'pacote': pkt,
                        'sucesso': False,
                        'buffer_atual': len(buffer_roteador),
                        'delays': {'d_trans': d_trans, 'd_proc': D_PROC, 'd_prop': D_PROP, 'd_fila': d_fila, 'd_nodal': d_nodal},
                        'log': f"⚠️ [TCP OVERFLOW] Buffer cheio! Pacote '{payload}' de {src} descartado. Reenviando..."
                    })

                    if len(buffer_roteador) > 0:
                        pkt_proc = buffer_roteador.pop(0)
                        target = pkt_proc['dst']
                        caixas_entrada[target].append(pkt_proc)

                        eventos.append({
                            'fase': 'IDA',
                            'tipo': 'entrega_destino',
                            'pacote': pkt_proc,
                            'buffer_atual': len(buffer_roteador),
                            'log': f"📬 [REPASSE] Pacote '{pkt_proc['payload']}' entregue a {target}. Buffer: {len(buffer_roteador)}/{buffer_limit}"
                        })

                        ack_pkt = {'id': f"ACK-{pkt_proc['id']}", 'tipo': 'ACK', 'src': target, 'dst': pkt_proc['src'], 'payload': f"ACK-{pkt_proc['seq']}"}
                        eventos.append({
                            'fase': 'VOLTA',
                            'tipo': 'entrega_ack',
                            'pacote': ack_pkt,
                            'buffer_atual': len(buffer_roteador),
                            'log': f"✅ [CONFIRMADO] {pkt_proc['src']} recebeu ACK de {target}."
                        })

    # 3. ESVAZIAMENTO DA FILA RESTANTE
    while len(buffer_roteador) > 0:
        pkt_proc = buffer_roteador.pop(0)
        target = pkt_proc['dst']
        caixas_entrada[target].append(pkt_proc)

        eventos.append({
            'fase': 'IDA',
            'tipo': 'entrega_destino',
            'pacote': pkt_proc,
            'buffer_atual': len(buffer_roteador),
            'log': f"📬 [REPASSE] Pacote '{pkt_proc['payload']}' entregue a {target}. Buffer: {len(buffer_roteador)}/{buffer_limit}"
        })

        ack_pkt = {'id': f"ACK-{pkt_proc['id']}", 'tipo': 'ACK', 'src': target, 'dst': pkt_proc['src'], 'payload': f"ACK-{pkt_proc['seq']}"}
        eventos.append({
            'fase': 'VOLTA',
            'tipo': 'entrega_ack',
            'pacote': ack_pkt,
            'buffer_atual': len(buffer_roteador),
            'log': f"✅ [CONFIRMADO] {pkt_proc['src']} recebeu ACK de {target}."
        })

    # 4. RECONSTITUIÇÃO ORGANIZADA POR REMETENTE
    mensagens_estruturadas = {}
    for user, pkts_recebidos in caixas_entrada.items():
        agrupado_por_origem = {}
        for pkt in pkts_recebidos:
            remetente = pkt['src']
            if remetente not in agrupado_por_origem:
                agrupado_por_origem[remetente] = []
            agrupado_por_origem[remetente].append(pkt['payload'])

        mensagens_estruturadas[user] = [
            {'de': rem, 'conteudo': "".join(fatias)}
            for rem, fatias in agrupado_por_origem.items()
        ]

    eventos.append({
        'tipo': 'fim',
        'mensagens': mensagens_estruturadas,
        'log': f"🏁 Simulação ({protocol}) finalizada!"
    })

    return jsonify(eventos)

if __name__ == '__main__':
    app.run(debug=True, port=5000)