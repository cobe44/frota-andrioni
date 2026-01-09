import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import xml.etree.ElementTree as ET
import html
import time
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(
    page_title="Gestão de Frota Pro | Andrioni", 
    layout="wide", 
    page_icon="🚛",
    initial_sidebar_state="expanded"
)

SHEET_NAME = "frota_db"

# ATUALIZADO: Adicionado 'responsavel' ao final da lista
LOG_COLUMNS = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "mecanico", "valor", "obs", "status", "responsavel"]

# --- CSS Otimizado ---
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    div[data-testid="stContainer"] { background-color: #16171D; border: 1px solid #31353F; border-radius: 8px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #4DB6AC !important; }
    .status-ok { color: #66BB6A; font-weight: bold; }
    .status-atencao { color: #FFA726; font-weight: bold; }
    .status-vencido { color: #EF5350; font-weight: bold; }
    .stButton button { width: 100%; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO GOOGLE SHEETS ---
@st.cache_resource
def connect_sheets():
    try:
        creds = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(creds)
        return gc.open(SHEET_NAME)
    except Exception as e:
        st.error(f"❌ Erro de conexão com Google Sheets: {e}")
        st.stop()

def init_db():
    try:
        sh = connect_sheets()
        existing = [ws.title for ws in sh.worksheets()]
        
        if "maintenance_logs" not in existing:
            ws = sh.add_worksheet(title="maintenance_logs", rows=100, cols=20)
            ws.append_row(LOG_COLUMNS)
        else:
            ws = sh.worksheet("maintenance_logs")
            if not ws.row_values(1): ws.append_row(LOG_COLUMNS)

        if "vehicles" not in existing:
            ws = sh.add_worksheet(title="vehicles", rows=100, cols=5)
            ws.append_row(["id_veiculo", "placa"])
            
        if "positions" not in existing:
            ws = sh.add_worksheet(title="positions", rows=100, cols=5)
            ws.append_row(["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])

        if "service_types" not in existing:
            ws = sh.add_worksheet(title="service_types", rows=50, cols=2)
            ws.append_row(["id", "nome_servico"])
            defaults = ["Troca de Óleo", "Pneus", "Freios", "Correia Dentada", "Filtros", "Suspensão", "Elétrica"]
            ws.append_rows([[i+1, s] for i, s in enumerate(defaults)])
            
    except Exception as e:
        st.toast(f"Erro init DB: {e}")

# --- CAMADA DE DADOS ---
def get_data(table_name):
    sh = connect_sheets()
    try:
        ws = sh.worksheet(table_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        if table_name == "maintenance_logs":
            if df.empty: return pd.DataFrame(columns=LOG_COLUMNS)
            # Adiciona colunas faltantes (Auto-Cura se adicionar campos novos)
            for col in LOG_COLUMNS:
                if col not in df.columns: df[col] = ""
            df = df[LOG_COLUMNS] # Ordena
            for c in ['km_realizada', 'proxima_km', 'valor', 'id']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
        if table_name == "positions" and not df.empty:
            df['odometro'] = pd.to_numeric(df['odometro'], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
        return df
    except Exception as e:
        if table_name == "maintenance_logs": return pd.DataFrame(columns=LOG_COLUMNS)
        return pd.DataFrame()

def salvar_posicoes_otimizado(novas_posicoes):
    if not novas_posicoes: return 0
    sh = connect_sheets()
    ws = sh.worksheet("positions")
    try:
        dados_atuais = ws.get_all_records()
        df_atual = pd.DataFrame(dados_atuais)
        df_novo = pd.DataFrame(novas_posicoes, columns=["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])
        
        df_final = pd.concat([df_atual, df_novo], ignore_index=True)
        df_final['timestamp'] = pd.to_datetime(df_final['timestamp'], errors='coerce')
        
        if 'id_pacote' in df_final.columns:
            df_final = df_final.drop_duplicates(subset=['id_pacote'], keep='last')
        
        corte = datetime.now() - timedelta(hours=48)
        df_final = df_final[df_final['timestamp'] >= corte].sort_values('timestamp')
        
        ws.clear()
        ws.append_row(["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])
        df_final['timestamp'] = df_final['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if not df_final.empty: ws.append_rows(df_final.values.tolist())  
        return len(df_novo)
    except: return 0

# --- CRUD (ATUALIZADO COM RESPONSAVEL) ---

def get_next_id(ws):
    try:
        col = ws.col_values(1)
        nums = [int(x) for x in col if str(x).isdigit()]
        return max(nums) + 1 if nums else 1
    except: return 1

def add_maintenance(placa, tipo, km, data, prox, mec, valor, obs, status, resp):
    sh = connect_sheets()
    ws = sh.worksheet("maintenance_logs")
    new_id = get_next_id(ws)
    # Ordem: id, placa, tipo, km, data, prox, mec, valor, obs, status, responsavel
    row = [new_id, placa, tipo, km, str(data), prox, mec, valor, obs, status, resp]
    ws.append_row(row)

def update_maintenance_full(id_m, tipo, km, data, prox, mec, valor, obs, status, resp):
    sh = connect_sheets()
    ws = sh.worksheet("maintenance_logs")
    try:
        cell = ws.find(str(id_m), in_column=1)
        if cell:
            # Atualiza colunas da 3 até a 11
            vals = [tipo, km, str(data), prox, mec, valor, obs, status, resp]
            for i, val in enumerate(vals):
                ws.update_cell(cell.row, 3+i, val)
    except Exception as e: st.error(f"Erro update: {e}")

def realizar_manutencao(id_m, data_real, valor_real, obs_real):
    sh = connect_sheets()
    ws = sh.worksheet("maintenance_logs")
    try:
        cell = ws.find(str(id_m), in_column=1)
        if cell:
            ws.update_cell(cell.row, 5, str(data_real))
            ws.update_cell(cell.row, 8, valor_real)
            obs_ant = ws.cell(cell.row, 9).value
            nova_obs = f"{obs_ant} | Fechamento: {obs_real}" if obs_ant else obs_real
            ws.update_cell(cell.row, 9, nova_obs)
            ws.update_cell(cell.row, 10, "Concluido")
    except: pass

def delete_maintenance(id_m):
    sh = connect_sheets()
    ws = sh.worksheet("maintenance_logs")
    try:
        cell = ws.find(str(id_m), in_column=1)
        if cell: ws.delete_rows(cell.row)
    except: pass

# --- INTEGRAÇÃO SASCAR ---
def soap_request(method, user, pwd, params_body):
    url = "https://sasintegra.sascar.com.br/SasIntegra/SasIntegraWSService?wsdl"
    headers = {'Content-Type': 'text/xml; charset=utf-8'}
    ns = "http://webservice.web.integracao.sascar.com.br/"
    envelope = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="{ns}">
       <soapenv:Header/>
       <soapenv:Body><web:{method}><usuario>{html.escape(user)}</usuario><senha>{html.escape(pwd)}</senha>{params_body}</web:{method}></soapenv:Body></soapenv:Envelope>"""
    try:
        r = requests.post(url, data=envelope, headers=headers, timeout=40)
        return r.status_code, r.content
    except Exception as e: return 0, str(e).encode()

def baixar_posicoes_recentes(user, pwd):
    df_v = get_data("vehicles")
    if df_v.empty: return 0
    v_map = dict(zip(df_v['id_veiculo'].astype(str), df_v['placa']))
    code, xml = soap_request("obterPacotePosicoes", user, pwd, "<quantidade>500</quantidade>")
    if code != 200: return 0
    novas = []
    try:
        root = ET.fromstring(xml)
        for item in root.iter():
            if item.tag.endswith('return'):
                d = {child.tag.split('}')[-1]: child.text for child in item if child.text}
                pid, vid, dt = d.get('idPacote'), d.get('idVeiculo'), d.get('dataPosicao')
                odo = float(d.get('odometro', 0))
                if pid and vid and dt:
                    placa = v_map.get(vid, "Desconhecido")
                    ts = dt.split('.')[0].replace('T', ' ')
                    novas.append([pid, vid, placa, ts, odo])
    except: pass
    if novas: return salvar_posicoes_otimizado(novas)
    return 0

def baixar_veiculos_auto(user, pwd):
    code, xml = soap_request("obterVeiculos", user, pwd, "<quantidade>1000</quantidade><idVeiculo>0</idVeiculo>")
    if code != 200: return False
    novos = []
    try:
        root = ET.fromstring(xml)
        for item in root.iter():
            if item.tag.endswith('return'):
                vid, vplaca = None, None
                for child in item:
                    tag = child.tag.split('}')[-1]
                    if tag == 'idVeiculo': vid = child.text
                    if tag == 'placa': vplaca = child.text
                if vid and vplaca: novos.append([vid, vplaca])
        if novos:
            sh = connect_sheets()
            ws = sh.worksheet("vehicles")
            ws.clear(); ws.append_row(["id_veiculo", "placa"]); ws.append_rows(novos)
            return True
    except: return False
    return False

# --- APP PRINCIPAL ---
def main():
    init_db()
    
    if 'edit_mode' not in st.session_state: st.session_state.edit_mode = False
    if 'edit_data' not in st.session_state: st.session_state.edit_data = {}
    if 'realizar_id' not in st.session_state: st.session_state.realizar_id = None
    if 'u' not in st.session_state: st.session_state.u = ''
    if 'p' not in st.session_state: st.session_state.p = ''
    if 'last_update' not in st.session_state: st.session_state.last_update = datetime.now() - timedelta(hours=3)

    st.sidebar.title("🚛 Frota Manager v3.3")
    with st.sidebar.expander("⚙️ Conexão Sascar"):
        u = st.text_input("Usuário", value=st.session_state.u)
        p = st.text_input("Senha", type="password", value=st.session_state.p)
        if st.button("Conectar / Baixar Veículos"):
            st.session_state.u = u; st.session_state.p = p
            if baixar_veiculos_auto(u, p): st.success("OK!"); time.sleep(0.5); st.rerun()

    agora = datetime.now()
    if st.session_state.u and (agora - st.session_state.last_update).total_seconds() > 3600:
        baixar_posicoes_recentes(st.session_state.u, st.session_state.p)
        st.session_state.last_update = agora
        st.rerun()

    c_top1, c_top2 = st.columns([4, 1])
    c_top1.title("Painel de Controle Andrioni")
    if c_top2.button("🔄 Atualizar Dados"):
        if st.session_state.u:
            n = baixar_posicoes_recentes(st.session_state.u, st.session_state.p)
            st.toast(f"{n} registros processados.")
            st.session_state.last_update = agora
            time.sleep(1); st.rerun()
        else: st.warning("Configure a Sascar na lateral.")

    df_v = get_data("vehicles")
    veiculos = df_v['placa'].tolist() if not df_v.empty else []
    df_pos = get_data("positions")
    df_maint = get_data("maintenance_logs")

    # --- FORMULÁRIO DE PROGRAMAÇÃO ---
    with st.expander("➕ Nova Programação / Lançamento", expanded=st.session_state.edit_mode):
        d = st.session_state.edit_data if st.session_state.edit_mode else {}
        
        c_veic, _ = st.columns([1, 2])
        idx_v = veiculos.index(d.get('placa')) if (st.session_state.edit_mode and d.get('placa') in veiculos) else 0
        sel_placa = c_veic.selectbox("Veículo", veiculos, index=idx_v, disabled=st.session_state.edit_mode)
        
        km_sugerido = 0.0
        if not df_pos.empty:
            pos = df_pos[df_pos['placa'] == sel_placa]
            if not pos.empty: km_sugerido = float(pos['odometro'].max())

        km_input_value = float(d.get('km_realizada', km_sugerido)) if st.session_state.edit_mode else km_sugerido

        with st.form("form_prog"):
            c1, c2 = st.columns(2)
            tipos = get_data("service_types")['nome_servico'].tolist() if not get_data("service_types").empty else ["Troca de Óleo"]
            idx_t = tipos.index(d.get('tipo_servico')) if (st.session_state.edit_mode and d.get('tipo_servico') in tipos) else 0
            sel_servico = c1.selectbox("Serviço", tipos, index=idx_t)
            
            status_ini = d.get('status', 'Agendado')
            is_done = c2.checkbox("Já foi realizada? (Lançar histórico)", value=(status_ini=='Concluido'))
            
            st.markdown("---")
            c3, c4 = st.columns(2)
            input_km_base = c3.number_input("KM Base", value=km_input_value, step=100.0)
            
            prox_db = float(d.get('proxima_km', 0))
            intervalo_padrao = (prox_db - input_km_base) if (st.session_state.edit_mode and prox_db > 0) else 10000.0
            input_intervalo = c4.number_input("Intervalo (KM)", value=intervalo_padrao, step=1000.0)
            
            km_final_programado = input_km_base + input_intervalo
            st.caption(f"🏁 Próxima troca: **{km_final_programado:,.0f} km**")

            # LINHA DE DADOS FINANCEIROS E RESPONSÁVEL
            c5, c6, c7 = st.columns(3)
            try: dt_ini = datetime.strptime(str(d.get('data_realizada', '')), '%Y-%m-%d').date()
            except: dt_ini = datetime.now()
            
            input_data = c5.date_input("Data", dt_ini, format="DD/MM/YYYY")
            input_valor = c6.number_input("Valor (R$)", value=float(d.get('valor', 0)))
            
            # --- NOVO CAMPO: RESPONSÁVEL ---
            input_resp = c7.text_input("Responsável", value=d.get('responsavel', ''))

            obs = st.text_area("Obs", value=d.get('obs', ''), height=70)

            col_b1, col_b2 = st.columns([1, 5])
            if col_b1.form_submit_button("💾 Salvar"):
                status_final = "Concluido" if is_done else "Agendado"
                if st.session_state.edit_mode:
                    update_maintenance_full(d.get('id'), sel_servico, input_km_base, input_data, km_final_programado, "", input_valor, obs, status_final, input_resp)
                    st.success("Atualizado!")
                else:
                    add_maintenance(sel_placa, sel_servico, input_km_base, input_data, km_final_programado, "", input_valor, obs, status_final, input_resp)
                    st.success("Salvo!")
                st.session_state.edit_mode = False; st.session_state.edit_data = {}
                time.sleep(1); st.rerun()
            
            if st.session_state.edit_mode and col_b2.form_submit_button("❌ Cancelar"):
                 st.session_state.edit_mode = False; st.session_state.edit_data = {}; st.rerun()

    # --- MODAL DE REALIZAÇÃO ---
    if st.session_state.realizar_id:
        st.markdown("### 🚀 Finalizando Ordem")
        with st.container(border=True):
            item = df_maint[df_maint['id'] == st.session_state.realizar_id].iloc[0]
            with st.form("form_finalizar"):
                st.info(f"{item['tipo_servico']} - {item['placa']}")
                cr1, cr2 = st.columns(2)
                dt_fim = cr1.date_input("Data Realização", datetime.now(), format="DD/MM/YYYY")
                val_fim = cr2.number_input("Valor Final (R$)", min_value=0.0, value=float(item['valor']))
                obs_fim = st.text_input("Nota Final")
                if st.form_submit_button("✅ Concluir"):
                    realizar_manutencao(st.session_state.realizar_id, dt_fim, val_fim, obs_fim)
                    st.session_state.realizar_id = None; st.rerun()
            if st.button("Voltar"): st.session_state.realizar_id = None; st.rerun()

    st.divider()

    # --- DASHBOARD VISUAL (CARDS) ---
    tab_prog, tab_hist = st.tabs(["📅 Aberto", "✅ Histórico"])
    
    if not df_v.empty and not df_pos.empty:
        last_pos = df_pos.sort_values('timestamp').groupby('placa').tail(1)
        df_view = df_v.merge(last_pos[['placa', 'odometro']], on='placa', how='left')
    else:
        df_view = df_v.copy(); df_view['odometro'] = 0

    with tab_prog:
        if not df_maint.empty and 'status' in df_maint.columns:
            m_abertas = df_maint[df_maint['status'] != 'Concluido']
        else: m_abertas = pd.DataFrame()

        if m_abertas.empty: st.info("Nenhuma programação pendente.")
        else:
            km_dict = dict(zip(df_view['placa'], df_view['odometro']))
            for _, m in m_abertas.iterrows():
                placa = m['placa']
                km_atual = float(km_dict.get(placa, 0) or 0)
                meta_km = float(m['proxima_km'] or 0)
                restam = meta_km - km_atual
                
                if restam <= 0: status_cls, txt_status, icon = "status-vencido", f"VENCIDO: {abs(restam):,.0f} KM", "🚨"
                elif restam < 1000: status_cls, txt_status, icon = "status-atencao", f"ATENÇÃO: {restam:,.0f} KM", "⚠️"
                else: status_cls, txt_status, icon = "status-ok", f"NO PRAZO: {restam:,.0f} KM", "🟢"

                with st.container():
                    c_info, c_kpi, c_act = st.columns([3, 2, 1])
                    with c_info:
                        st.subheader(f"{icon} {placa}")
                        st.markdown(f"**Serviço:** {m['tipo_servico']}")
                        # EXIBIR RESPONSAVEL NO CARD TAMBÉM
                        resp_txt = m.get('responsavel', '') or 'Não inf.'
                        st.caption(f"Resp: {resp_txt} | Prev: {m['data_realizada']}")
                        st.caption(f"Obs: {m['obs']}")
                    with c_kpi:
                        st.markdown(f"Meta: **{meta_km:,.0f}** | Atual: **{km_atual:,.0f}**")
                        st.markdown(f"<span class='{status_cls}'>{txt_status}</span>", unsafe_allow_html=True)
                        st.markdown(f"Orçado: **R$ {m['valor']}**")
                    with c_act:
                        if st.button("✅", key=f"do_{m['id']}"): st.session_state.realizar_id = m['id']; st.rerun()
                        if st.button("✏️", key=f"ed_{m['id']}"): st.session_state.edit_mode=True; st.session_state.edit_data=m.to_dict(); st.rerun()
                        if st.button("🗑️", key=f"del_{m['id']}"): delete_maintenance(m['id']); st.rerun()

    with tab_hist:
        if not df_maint.empty and 'status' in df_maint.columns:
            m_conc = df_maint[df_maint['status'] == 'Concluido']
            st.dataframe(m_conc.drop(columns=['id'], errors='ignore'), use_container_width=True)
        else: st.info("Sem histórico.")

if __name__ == "__main__":
    main()