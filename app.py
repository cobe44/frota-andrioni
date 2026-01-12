import streamlit as st
import pandas as pd
import gspread
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
    
    /* Card de Status */
    .status-card { 
        padding: 12px 15px; 
        border-radius: 8px; 
        border: 1px solid #e0e0e0; 
        background: white; 
        margin-bottom: 8px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Tipografia dos Status */
    .status-vencido { color: #d9534f; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-atencao { color: #f0ad4e; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-ok { color: #5cb85c; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    
    .placa-title { font-size: 1.1rem; font-weight: 700; color: #333; }
    .meta-info { color: #666; font-size: 0.85rem; margin-top: 4px; }
    
    /* Remove padding excessivo do topo */
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
            r = requests.post(self.url, data=envelope, headers=self.headers, timeout=30)
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

    def get_positions(self, qtd=500):
        code, xml = self._send_soap("obterPacotePosicoes", f"<quantidade>{qtd}</quantidade>")
        if code != 200: return []
        posicoes = []
        try:
            root = ET.fromstring(xml)
            for item in root.iter():
                if item.tag.endswith('return'):
                    d = {child.tag.split('}')[-1]: child.text for child in item}
                    if 'idPacote' in d and 'idVeiculo' in d:
                        ts = d.get('dataPosicao', '').split('.')[0].replace('T', ' ')
                        odo = float(d.get('odometro', 0))
                        posicoes.append({
                            'id_pacote': d['idPacote'],
                            'id_veiculo': d['idVeiculo'],
                            'timestamp': ts,
                            'odometro': odo
                        })
            return posicoes
        except: return []

class FleetDatabase:
    def __init__(self, sheet_name="frota_db"):
        self.sheet_name = sheet_name
        # Ordem exata: A=id, B=placa, C=tipo, D=km_real, E=data, F=prox, G=resp, H=valor, I=obs, J=status
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

    def get_dataframe(self, worksheet_name):
        sh = self._get_connection()
        if not sh: return pd.DataFrame()
        try:
            ws = sh.worksheet(worksheet_name)
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            if worksheet_name == "maintenance_logs":
                if df.empty: return pd.DataFrame(columns=self.log_cols)
                for col in self.log_cols:
                    if col not in df.columns: df[col] = ""
            return df
        except: return pd.DataFrame()

    def sync_sascar_data(self, sascar_service: SascarService):
        status_msg = st.empty()
        status_msg.info("⏳ Sincronizando...")
        veiculos_api = sascar_service.get_vehicles()
        if veiculos_api:
            sh = self._get_connection()
            ws = sh.worksheet("vehicles")
            ws.clear()
            ws.append_row(["id_veiculo", "placa"])
            ws.append_rows(veiculos_api)
        
        pos_api = sascar_service.get_positions(qtd=500)
        if pos_api:
            df_v = self.get_dataframe("vehicles")
            map_placa = dict(zip(df_v['id_veiculo'].astype(str), df_v['placa']))
            novos_dados = []
            for p in pos_api:
                placa = map_placa.get(str(p['id_veiculo']), "Desconhecido")
                novos_dados.append([
                    p['id_pacote'], p['id_veiculo'], placa, p['timestamp'], p['odometro']
                ])
            sh = self._get_connection()
            ws_pos = sh.worksheet("positions")
            ws_pos.append_rows(novos_dados)
            status_msg.success(f"✅ {len(novos_dados)} novas posições.")
            time.sleep(2); status_msg.empty()
            return True
        status_msg.warning("⚠️ Nada novo."); time.sleep(2); status_msg.empty()
        return False

    def add_log(self, data_dict):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
        col_ids = ws.col_values(1)
        next_id = 1
        if len(col_ids) > 1:
            ids = [int(x) for x in col_ids[1:] if str(x).isdigit()]
            if ids: next_id = max(ids) + 1
            
        row = [
            next_id, 
            data_dict['placa'], 
            data_dict['tipo'], 
            data_dict['km'],
            str(data_dict['data']), 
            data_dict['prox_km'], 
            data_dict['resp'],   # Coluna G
            data_dict['valor'], 
            data_dict['obs'], 
            data_dict['status']
        ]
        ws.append_row(row)

    def update_log_status(self, log_id, data_real, valor_final, obs_final):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
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
        ws = sh.worksheet("maintenance_logs")
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                ws.delete_rows(cell.row)
                return True
        except: return False

    def edit_log_full(self, log_id, novos_dados):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
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
    
    # Session State
    if 'last_sync' not in st.session_state: st.session_state.last_sync = None
    
    # --- SIDEBAR ---
    with st.sidebar:
        st.header("Gestão de Frota")
        default_user = st.secrets.get("sascar", {}).get("user", "")
        default_pass = st.secrets.get("sascar", {}).get("password", "")
        with st.expander("🔐 Sascar", expanded=not default_user):
            sascar_user = st.text_input("Usuário", value=default_user)
            sascar_pass = st.text_input("Senha", type="password", value=default_pass)
        
        if st.button("🔄 Sincronizar Agora", use_container_width=True):
            if sascar_user and sascar_pass:
                svc = SascarService(sascar_user, sascar_pass)
                if db.sync_sascar_data(svc):
                    st.session_state.last_sync = datetime.now()
                    st.rerun()

    st.title("🚛 Painel de Controle")

    # Carrega Dados
    df_v = db.get_dataframe("vehicles")
    df_pos = db.get_dataframe("positions")
    df_logs = db.get_dataframe("maintenance_logs")

    # Cruza dados para obter KM atual
    if not df_pos.empty:
        df_pos['timestamp'] = pd.to_datetime(df_pos['timestamp'], errors='coerce')
        df_pos['odometro'] = pd.to_numeric(df_pos['odometro'], errors='coerce')
        last_pos = df_pos.sort_values('timestamp').groupby('id_veiculo').tail(1)
        if not df_v.empty:
            df_v['id_veiculo'] = df_v['id_veiculo'].astype(str)
            last_pos['id_veiculo'] = last_pos['id_veiculo'].astype(str)
            df_frota = pd.merge(df_v, last_pos[['id_veiculo', 'odometro']], on='id_veiculo', how='left')
            df_frota['odometro'] = df_frota['odometro'].fillna(0)
        else:
            df_frota = pd.DataFrame(columns=['placa', 'odometro'])
    else:
        df_frota = df_v.copy() if not df_v.empty else pd.DataFrame(columns=['placa'])
        df_frota['odometro'] = 0

    # --- ABAS (LAYOUT ORIGINAL) ---
    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # --- ABA 1: PENDÊNCIAS ---
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            if not pendentes.empty:
                km_map = dict(zip(df_frota['placa'], df_frota['odometro']))
                pendentes['km_restante'] = pd.to_numeric(pendentes['proxima_km'], errors='coerce') - pendentes['placa'].map(km_map).fillna(0)
                pendentes = pendentes.sort_values('km_restante')

                for index, row in pendentes.iterrows():
                    placa = row['placa']
                    km_atual = float(km_map.get(placa, 0))
                    meta_km = float(row['proxima_km']) if row['proxima_km'] != '' else 0
                    restante = meta_km - km_atual
                    
                    if restante < 0:
                        s_cls = "status-vencido"; s_txt = f"🚨 VENCIDO ({abs(restante):,.0f} KM)"; b_col = "#d9534f"
                    elif restante < 1000:
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
                        
                        # Ações
                        c1, c2, c3 = st.columns([2, 2, 0.5])
                        
                        # Baixar
                        with c1.expander("✅ Baixar O.S."):
                            with st.form(key=f"bx_{row['id']}"):
                                dt_bx = st.date_input("Data Real", datetime.now())
                                vl_bx = st.number_input("Valor R$", value=float(row['valor']) if row['valor'] else 0.0)
                                obs_bx = st.text_input("Obs")
                                if st.form_submit_button("Concluir"):
                                    db.update_log_status(row['id'], dt_bx, vl_bx, obs_bx)
                                    st.success("Ok!"); time.sleep(0.5); st.rerun()

                        # Editar
                        with c2.expander("✏️ Editar"):
                            with st.form(key=f"ed_{row['id']}"):
                                e_placa = st.selectbox("Placa", df_frota['placa'].unique(), index=df_frota['placa'].tolist().index(row['placa']) if row['placa'] in df_frota['placa'].tolist() else 0)
                                e_tipo = st.text_input("Serviço", value=row['tipo_servico'])
                                e_resp = st.text_input("Resp", value=row['responsavel'])
                                e_km = st.number_input("KM Base", value=float(row['km_realizada']) if row['km_realizada'] else 0.0)
                                e_prox = st.number_input("Meta KM", value=float(row['proxima_km']) if row['proxima_km'] else 0.0)
                                e_val = st.number_input("Valor", value=float(row['valor']) if row['valor'] else 0.0)
                                e_obs = st.text_area("Obs", value=row['obs'])
                                if st.form_submit_button("Salvar"):
                                    novos = {'placa':e_placa, 'tipo':e_tipo, 'resp':e_resp, 'km':e_km, 'prox_km':e_prox, 'valor':e_val, 'obs':e_obs}
                                    db.edit_log_full(row['id'], novos)
                                    st.rerun()

                        # Excluir
                        if c3.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                            db.delete_log(row['id']); st.rerun()
            else:
                st.info("Nenhuma pendência.")

    # --- ABA 2: NOVO LANÇAMENTO ---
    with tab_novo:
        st.subheader("Registrar Manutenção")
        
        # Keys para limpeza
        keys_clear = ["n_placa", "n_serv", "n_km", "n_inter", "n_dt", "n_val", "n_resp", "n_obs", "n_done", "n_agendar"]

        with st.form("form_novo", clear_on_submit=False):
            l_placas = df_frota['placa'].unique().tolist()
            c1, c2 = st.columns(2)
            sel_placa = c1.selectbox("Placa", l_placas, key="n_placa")
            sel_servico = c2.selectbox("Serviço", ["Troca de Óleo", "Pneus", "Freios", "Correia", "Filtros", "Suspensão", "Elétrica", "Outros"], key="n_serv")
            
            c3, c4 = st.columns(2)
            km_base = c3.number_input("KM na data do serviço (Manual)", value=0.0, step=100.0, key="n_km")
            intervalo = c4.number_input("Intervalo (KM)", value=10000.0, step=1000.0, key="n_inter")
            
            prox_calc = km_base + intervalo
            st.caption(f"📅 Próxima prevista: **{prox_calc:,.0f} KM**")

            c5, c6, c7 = st.columns(3)
            dt_reg = c5.date_input("Data", datetime.now(), key="n_dt")
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
                
                # 1. Registro Atual
                d1 = {
                    "placa": sel_placa, "tipo": sel_servico, "km": km_log,
                    "data": dt_reg, "prox_km": prox_calc, "valor": val_reg, 
                    "obs": obs_reg, "resp": resp_reg, "status": stt
                }
                db.add_log(d1)
                
                # 2. Registro Futuro
                if is_done and do_sched:
                    d2 = {
                        "placa": sel_placa, "tipo": sel_servico, "km": "", "data": "",
                        "prox_km": prox_calc, "valor": 0, "obs": "Agendamento automático.",
                        "resp": "", "status": "Agendado"
                    }
                    db.add_log(d2)
                
                st.toast("Salvo com sucesso!")
                
                # Limpa chaves
                for k in keys_clear:
                    if k in st.session_state: del st.session_state[k]
                
                time.sleep(1)
                st.rerun()

    # --- ABA 3: HISTÓRICO ---
    with tab_hist:
        if not df_logs.empty:
            h = df_logs[df_logs['status'] == 'Concluido'].sort_values('id', ascending=False)
            st.dataframe(h, use_container_width=True, hide_index=True)
        else:
            st.info("Histórico vazio.")

if __name__ == "__main__":
    main()
