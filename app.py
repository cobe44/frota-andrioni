import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
import requests
import xml.etree.ElementTree as ET
import html
from datetime import datetime, timedelta
import time

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Gestão de Frota | Andrioni",
    layout="wide",
    page_icon="🚛"
)

# --- 2. CSS "LISO" E LIMPO ---
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    .status-card { 
        padding: 12px 15px; 
        border-radius: 8px; 
        border: 1px solid #e0e0e0; 
        background: white; 
        margin-bottom: 8px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .status-vencido { color: #d9534f; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-atencao { color: #f0ad4e; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-ok { color: #5cb85c; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .placa-title { font-size: 1.1rem; font-weight: 700; color: #333; }
    .meta-info { color: #666; font-size: 0.85rem; margin-top: 4px; }
    .block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)

# --- 3. SERVIÇOS (SASCAR & DATABASE) ---
class SascarService:
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.url = "https://sasintegra.sascar.com.br/SasIntegra/SasIntegraWSService?wsdl"
        self.headers = {'Content-Type': 'text/xml; charset=utf-8'}
        self.ns = "http://webservice.web.integracao.sascar.com.br/"

    def _send_soap(self, method, body_params):
        envelope = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="{self.ns}">
           <soapenv:Header/>
           <soapenv:Body><web:{method}>
               <usuario>{html.escape(self.user)}</usuario>
               <senha>{html.escape(self.password)}</senha>
               {body_params}
           </web:{method}></soapenv:Body></soapenv:Envelope>"""
        try:
            r = requests.post(self.url, data=envelope, headers=self.headers, timeout=40)
            return r.status_code, r.content
        except Exception as e:
            return 0, str(e)

    def get_vehicles(self):
        code, xml = self._send_soap("obterVeiculos", "<quantidade>1000</quantidade><idVeiculo>0</idVeiculo>")
        if code != 200: return []
        veiculos = []
        try:
            root = ET.fromstring(xml)
            for item in root.iter():
                if item.tag.endswith('return'):
                    d = {child.tag.split('}')[-1]: child.text for child in item}
                    if 'idVeiculo' in d and 'placa' in d:
                        veiculos.append([d['idVeiculo'], d['placa']])
            return veiculos
        except: return []

    def get_positions(self, qtd=1000):
        code, xml = self._send_soap("obterPacotePosicoes", f"<quantidade>{qtd}</quantidade>")
        if code != 200: return []
        posicoes = []
        try:
            root = ET.fromstring(xml)
            for item in root.iter():
                if item.tag.endswith('return'):
                    d = {child.tag.split('}')[-1]: child.text for child in item}
                    if 'idPacote' in d and 'idVeiculo' in d:
                        raw_date = d.get('dataPosicao', '')
                        ts = ""
                        if raw_date:
                            try:
                                clean_date = raw_date.split('.')[0]
                                dt_obj = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
                                dt_obj = dt_obj - timedelta(hours=3)
                                ts = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                            except:
                                ts = raw_date.replace('T', ' ')
                        odo = float(d.get('odometro', 0))
                        posicoes.append({
                            'id_pacote': d['idPacote'], 'id_veiculo': d['idVeiculo'],
                            'timestamp': ts, 'odometro': odo
                        })
            return posicoes
        except: return []

class FleetDatabase:
    def __init__(self, sheet_name="frota_db"):
        self.sheet_name = sheet_name
        self.log_cols = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "responsavel", "valor", "obs", "status"]

    @st.cache_resource
    def _get_connection(_self):
        try:
            creds = st.secrets["gcp_service_account"]
            gc = gspread.service_account_from_dict(creds)
            return gc.open(_self.sheet_name)
        except Exception as e:
            st.error(f"Erro conexão Sheets: {e}")
            return None

    def _safe_get_worksheet(self, sh, title):
        for attempt in range(4):
            try: return sh.worksheet(title)
            except APIError: time.sleep(2 * (attempt + 1))
            except: return None
        return None
    
    def _safe_clear(self, ws):
        for attempt in range(4):
            try: ws.clear(); return True
            except APIError: time.sleep(2)
        return False

    def _safe_append(self, ws, rows):
        for attempt in range(4):
            try: ws.append_rows(rows); return True
            except APIError: time.sleep(2)
        return False

    def get_dataframe(self, worksheet_name):
        sh = self._get_connection()
        if not sh: return pd.DataFrame()
        try:
            ws = self._safe_get_worksheet(sh, worksheet_name)
            if not ws: return pd.DataFrame()
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            if worksheet_name == "maintenance_logs":
                if df.empty: return pd.DataFrame(columns=self.log_cols)
                for col in self.log_cols:
                    if col not in df.columns: df[col] = ""
            return df
        except: return pd.DataFrame()

    def get_services_list(self):
        defaults = ["Troca de Óleo", "Pneus", "Freios", "Correia", "Filtros", "Suspensão", "Elétrica", "Outros"]
        sh = self._get_connection()
        if not sh: return defaults
        try:
            ws = self._safe_get_worksheet(sh, "service_types")
            if not ws: return defaults
            vals = ws.col_values(2) 
            if len(vals) > 1: return vals[1:]
            return defaults
        except: return defaults

    # --- ATUALIZAR KM MANUAL ---
    def update_manual_km(self, placa, novo_km):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "veiculos_manuais")
        if not ws: return False
        try:
            cell = ws.find(placa, in_column=1)
            if cell:
                ws.update_cell(cell.row, 2, novo_km) # Atualiza coluna B (odometro)
            else:
                # Se não achar a placa, cria novo
                ws.append_row([placa, novo_km])
            return True
        except: return False

    def sync_sascar_data(self, sascar_service: SascarService):
        status_msg = st.empty()
        status_msg.info("⏳ Conectando à Sascar...")
        
        # 1. Veículos
        veiculos_api = sascar_service.get_vehicles()
        if veiculos_api:
            sh = self._get_connection()
            ws = self._safe_get_worksheet(sh, "vehicles")
            if ws:
                self._safe_clear(ws)
                ws.append_row(["id_veiculo", "placa"])
                self._safe_append(ws, veiculos_api)
        else:
            status_msg.error("❌ Erro ao listar veículos.")
            time.sleep(2); return False
        
        # 2. LOOP (Fila com limite de 5 pacotes)
        max_loops = 5
        todas_novas_posicoes = []
        for i in range(max_loops):
            msg = f"⏳ Baixando pacote {i+1}/{max_loops}..."
            status_msg.info(msg)
            lote = sascar_service.get_positions(qtd=1000)
            if not lote: break
            todas_novas_posicoes.extend(lote)
            if len(lote) < 1000: break
            time.sleep(1) 
        
        if todas_novas_posicoes:
            status_msg.info(f"💾 Salvando {len(todas_novas_posicoes)} registros...")
            df_v = self.get_dataframe("vehicles")
            map_placa = {}
            if not df_v.empty:
                map_placa = dict(zip(df_v['id_veiculo'].astype(str), df_v['placa']))
            
            dados_formatados = []
            for p in todas_novas_posicoes:
                placa = map_placa.get(str(p['id_veiculo']), "Desconhecido")
                dados_formatados.append([p['id_pacote'], p['id_veiculo'], placa, p['timestamp'], p['odometro']])
            
            sh = self._get_connection()
            ws_pos = self._safe_get_worksheet(sh, "positions")
            if ws_pos:
                try: dados_existentes = ws_pos.get_all_records()
                except APIError: time.sleep(3); dados_existentes = ws_pos.get_all_records()
                
                colunas = ["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"]
                df_antigo = pd.DataFrame(dados_existentes)
                df_novo = pd.DataFrame(dados_formatados, columns=colunas)
                df_total = pd.concat([df_antigo, df_novo])
                
                if not df_total.empty:
                    df_total['timestamp'] = pd.to_datetime(df_total['timestamp'], errors='coerce')
                    df_limpo = df_total.sort_values('timestamp').drop_duplicates(subset=['placa'], keep='last')
                    df_limpo['timestamp'] = df_limpo['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    self._safe_clear(ws_pos)
                    ws_pos.append_row(colunas)
                    self._safe_append(ws_pos, df_limpo.values.tolist())
                    status_msg.success(f"✅ Sucesso! Base atualizada.")
                else: status_msg.warning("⚠️ Dados vazios.")
        else: status_msg.success("✅ Tudo atualizado."); 
            
        time.sleep(2); status_msg.empty()
        return True

    def add_log(self, data_dict):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return
        col_ids = ws.col_values(1)
        next_id = 1
        if len(col_ids) > 1:
            ids = [int(x) for x in col_ids[1:] if str(x).isdigit()]
            if ids: next_id = max(ids) + 1
        row = [next_id, data_dict['placa'], data_dict['tipo'], data_dict['km'], str(data_dict['data']), data_dict['prox_km'], data_dict['resp'], data_dict['valor'], data_dict['obs'], data_dict['status']]
        ws.append_row(row)

    def update_log_status(self, log_id, data_real, valor_final, obs_final):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                ws.update_cell(cell.row, 5, str(data_real))
                ws.update_cell(cell.row, 8, valor_final)
                old_obs = ws.cell(cell.row, 9).value
                new_obs = f"{old_obs} | Baixa: {obs_final}" if old_obs else obs_final
                ws.update_cell(cell.row, 9, new_obs)
                ws.update_cell(cell.row, 10, "Concluido")
        except: pass

    def delete_log(self, log_id):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return False
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell: ws.delete_rows(cell.row); return True
        except: return False

    def edit_log_full(self, log_id, novos_dados):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return False
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                r = cell.row
                ws.update_cell(r, 2, novos_dados['placa'])
                ws.update_cell(r, 3, novos_dados['tipo'])
                ws.update_cell(r, 4, novos_dados['km'])
                ws.update_cell(r, 6, novos_dados['prox_km'])
                ws.update_cell(r, 7, novos_dados['resp'])
                ws.update_cell(r, 8, novos_dados['valor'])
                ws.update_cell(r, 9, novos_dados['obs'])
                return True
        except: return False

# --- 4. APP PRINCIPAL ---
def main():
    db = FleetDatabase()
    if 'last_sync' not in st.session_state: st.session_state.last_sync = None
    
    lista_servicos_db = db.get_services_list()

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("Gestão de Frota")
        
        # Sincronização Sascar
        default_user = st.secrets.get("sascar", {}).get("user", "")
        default_pass = st.secrets.get("sascar", {}).get("password", "")
        with st.expander("🔐 Sascar Sync"):
            sascar_user = st.text_input("Usuário", value=default_user)
            sascar_pass = st.text_input("Senha", type="password", value=default_pass)
            if st.button("🔄 Sincronizar Agora", use_container_width=True):
                if sascar_user and sascar_pass:
                    svc = SascarService(sascar_user, sascar_pass)
                    if db.sync_sascar_data(svc):
                        st.session_state.last_sync = datetime.now()
                        st.rerun()

        st.divider()
        
        # --- ATUALIZADOR DE KM MANUAL ---
        with st.expander("🚗 Atualizar KM Manual", expanded=True):
            # Carrega manuais para o selectbox
            df_manuais = db.get_dataframe("veiculos_manuais")
            lista_manuais = df_manuais['placa'].tolist() if not df_manuais.empty else []
            
            placa_manual = st.selectbox("Selecione Manual", lista_manuais)
            if placa_manual:
                # Tenta pegar KM atual
                km_atual_manual = 0.0
                linha_atual = df_manuais[df_manuais['placa'] == placa_manual]
                if not linha_atual.empty:
                    km_atual_manual = float(linha_atual.iloc[0]['odometro'])
                    
                novo_km_manual = st.number_input("Novo KM", value=km_atual_manual, step=100.0)
                if st.button("Salvar KM"):
                    if db.update_manual_km(placa_manual, novo_km_manual):
                        st.success("Atualizado!")
                        time.sleep(1); st.rerun()
            
            # Adicionar novo veiculo manual rapido
            with st.popover("➕ Cadastrar Novo Manual"):
                novo_placa = st.text_input("Nova Placa")
                novo_km_ini = st.number_input("KM Inicial", value=0.0)
                if st.button("Criar"):
                    if db.update_manual_km(novo_placa, novo_km_ini):
                        st.success("Criado!"); time.sleep(1); st.rerun()

    st.title("🚛 Painel de Controle")

    # --- CARREGAMENTO E FUSÃO DE DADOS ---
    # 1. Dados Sascar
    df_v_sascar = db.get_dataframe("vehicles")
    df_pos_sascar = db.get_dataframe("positions")
    
    # 2. Dados Manuais
    df_v_manual = db.get_dataframe("veiculos_manuais") # Colunas: placa, odometro
    
    # 3. Processamento Sascar
    mapa_km_total = {}
    
    # Processa Sascar
    if not df_pos_sascar.empty:
        df_pos_sascar['timestamp'] = pd.to_datetime(df_pos_sascar['timestamp'], errors='coerce')
        df_pos_sascar['odometro'] = pd.to_numeric(df_pos_sascar['odometro'], errors='coerce')
        last_pos = df_pos_sascar.sort_values('timestamp').groupby('id_veiculo').tail(1)
        
        # Mapeia placa -> odometro
        if not df_v_sascar.empty:
             # Mapa ID -> Placa
            map_id_placa = dict(zip(df_v_sascar['id_veiculo'].astype(str), df_v_sascar['placa']))
            for _, row in last_pos.iterrows():
                p_id = str(row['id_veiculo'])
                p_placa = map_id_placa.get(p_id, row['placa']) 
                mapa_km_total[p_placa] = row['odometro']

    # Processa Manuais (Adiciona ou Sobrescreve no mapa)
    if not df_v_manual.empty:
        for _, row in df_v_manual.iterrows():
            mapa_km_total[row['placa']] = float(row['odometro'])

    # Lista consolidada de todas as placas para os dropdowns
    todas_placas = list(mapa_km_total.keys())
    todas_placas.sort()

    df_logs = db.get_dataframe("maintenance_logs")

    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # --- ABA 1: PENDÊNCIAS ---
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            if not pendentes.empty:
                # Usa o mapa consolidado (Sascar + Manual)
                pendentes['km_restante'] = pd.to_numeric(pendentes['proxima_km'], errors='coerce') - pendentes['placa'].map(mapa_km_total).fillna(0)
                pendentes = pendentes.sort_values('km_restante')

                for index, row in pendentes.iterrows():
                    placa = row['placa']
                    km_atual = float(mapa_km_total.get(placa, 0)) # Pega do mapa geral
                    meta_km = float(row['proxima_km']) if row['proxima_km'] != '' else 0
                    restante = meta_km - km_atual
                    
                    if restante < 0:
                        s_cls = "status-vencido"; s_txt = f"🚨 VENCIDO ({abs(restante):,.0f} KM)"; b_col = "#d9534f"
                    elif restante < 3000:
                        s_cls = "status-atencao"; s_txt = f"⚠️ ATENÇÃO ({restante:,.0f} KM)"; b_col = "#f0ad4e"
                    else:
                        s_cls = "status-ok"; s_txt = f"🟢 NO PRAZO ({restante:,.0f} KM)"; b_col = "#5cb85c"

                    with st.container():
                        st.markdown(f"""
                        <div class="status-card" style="border-left: 5px solid {b_col}">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <span class="placa-title">{placa}</span>
                                <span class="{s_cls}">{s_txt}</span>
                            </div>
                            <div><b>{row['tipo_servico']}</b> <span style="color:#888">| Resp: {row['responsavel']}</span></div>
                            <div class="meta-info">Meta: {meta_km:,.0f} km | Atual: {km_atual:,.0f} km</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        c1, c2, c3 = st.columns([2, 2, 0.5])
                        with c1.expander("✅ Baixar O.S."):
                            with st.form(key=f"bx_{row['id']}"):
                                dt_bx = st.date_input("Data Real", datetime.now() - timedelta(hours=3))
                                vl_bx = st.number_input("Valor R$", value=float(row['valor']) if row['valor'] else 0.0)
                                obs_bx = st.text_input("Obs")
                                if st.form_submit_button("Concluir"):
                                    db.update_log_status(row['id'], dt_bx, vl_bx, obs_bx)
                                    st.success("Ok!"); time.sleep(0.5); st.rerun()

                        with c2.expander("✏️ Editar"):
                            with st.form(key=f"ed_{row['id']}"):
                                # Dropdown com TODAS as placas
                                e_placa = st.selectbox("Placa", todas_placas, index=todas_placas.index(row['placa']) if row['placa'] in todas_placas else 0)
                                
                                idx_serv = 0
                                if row['tipo_servico'] in lista_servicos_db:
                                    idx_serv = lista_servicos_db.index(row['tipo_servico'])
                                e_tipo = st.selectbox("Serviço", lista_servicos_db, index=idx_serv)
                                e_resp = st.text_input("Resp", value=row['responsavel'])
                                e_km = st.number_input("KM Base", value=float(row['km_realizada']) if row['km_realizada'] else 0.0)
                                e_prox = st.number_input("Meta KM", value=float(row['proxima_km']) if row['proxima_km'] else 0.0)
                                e_val = st.number_input("Valor", value=float(row['valor']) if row['valor'] else 0.0)
                                e_obs = st.text_area("Obs", value=row['obs'])
                                if st.form_submit_button("Salvar"):
                                    novos = {'placa':e_placa, 'tipo':e_tipo, 'resp':e_resp, 'km':e_km, 'prox_km':e_prox, 'valor':e_val, 'obs':e_obs}
                                    db.edit_log_full(row['id'], novos)
                                    st.rerun()

                        if c3.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                            db.delete_log(row['id']); st.rerun()
            else:
                st.info("Nenhuma pendência.")

    # --- ABA 2: NOVO LANÇAMENTO ---
    with tab_novo:
        st.subheader("Registrar Manutenção")
        keys_clear = ["n_placa", "n_serv", "n_km", "n_inter", "n_dt", "n_val", "n_resp", "n_obs", "n_done", "n_agendar"]
        with st.form("form_novo", clear_on_submit=False):
            c1, c2 = st.columns(2)
            # Dropdown com TODAS as placas
            sel_placa = c1.selectbox("Placa", todas_placas, key="n_placa")
            sel_servico = c2.selectbox("Serviço", lista_servicos_db, key="n_serv")
            c3, c4 = st.columns(2)
            km_base = c3.number_input("KM na data do serviço (Manual)", value=0.0, step=100.0, key="n_km")
            intervalo = c4.number_input("Intervalo (KM)", value=10000.0, step=1000.0, key="n_inter")
            prox_calc = km_base + intervalo
            st.caption(f"📅 Próxima prevista: **{prox_calc:,.0f} KM**")
            c5, c6, c7 = st.columns(3)
            dt_reg = c5.date_input("Data", datetime.now() - timedelta(hours=3), key="n_dt")
            val_reg = c6.number_input("Valor (R$)", value=0.0, key="n_val")
            resp_reg = c7.text_input("Responsável", key="n_resp")
            obs_reg = st.text_area("Obs", key="n_obs")
            st.divider()
            cc1, cc2 = st.columns(2)
            is_done = cc1.checkbox("✅ Já realizada (Histórico)", value=True, key="n_done")
            do_sched = cc2.checkbox("🔄 Criar próxima pendência?", value=True, key="n_agendar")
            if st.form_submit_button("💾 Salvar Registro"):
                stt = "Concluido" if is_done else "Agendado"
                km_log = km_base if is_done else ""
                d1 = {"placa": sel_placa, "tipo": sel_servico, "km": km_log, "data": dt_reg, "prox_km": prox_calc, "valor": val_reg, "obs": obs_reg, "resp": resp_reg, "status": stt}
                db.add_log(d1)
                if is_done and do_sched:
                    d2 = {"placa": sel_placa, "tipo": sel_servico, "km": "", "data": "", "prox_km": prox_calc, "valor": 0, "obs": "Agendamento automático.", "resp": "", "status": "Agendado"}
                    db.add_log(d2)
                st.toast("Salvo com sucesso!")
                for k in keys_clear:
                    if k in st.session_state: del st.session_state[k]
                time.sleep(1); st.rerun()

    # --- ABA 3: HISTÓRICO ---
    with tab_hist:
        if not df_logs.empty:
            h = df_logs[df_logs['status'] == 'Concluido'].sort_values('id', ascending=False)
            st.dataframe(h, use_container_width=True, hide_index=True)
        else:
            st.info("Histórico vazio.")

if __name__ == "__main__":
    main()
