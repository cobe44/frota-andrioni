import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import xml.etree.ElementTree as ET
import html
import time
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO INICIAL (Padrão Nativo) ---
st.set_page_config(
    page_title="Gestão de Frota | Andrioni", 
    layout="wide", 
    page_icon="🚛"
)

SHEET_NAME = "frota_db"
LOG_COLUMNS = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "mecanico", "valor", "obs", "status", "responsavel"]

# --- CSS MÍNIMO (Apenas para destacar status colorido) ---
# Não mexemos mais em fundo nem cor de letra geral.
st.markdown("""
<style>
    /* Cores apenas para os avisos de status (Vermelho, Amarelo, Verde) */
    .status-vencido { 
        color: #FF4B4B; 
        font-weight: 800; 
        text-transform: uppercase;
    }
    .status-atencao { 
        color: #FFA421; 
        font-weight: 800; 
        text-transform: uppercase; 
    }
    .status-ok { 
        color: #21C354; 
        font-weight: 800; 
        text-transform: uppercase; 
    }
    
    /* Pequeno ajuste para deixar os botões com largura total no celular */
    .stButton button { width: 100%; }
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
        st.error(f"❌ Erro de conexão: {e}")
        st.stop()

def init_db():
    try:
        sh = connect_sheets()
        existing = [ws.title for ws in sh.worksheets()]
        
        # Garante abas
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

# --- DADOS ---
def get_data(table_name):
    sh = connect_sheets()
    try:
        ws = sh.worksheet(table_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        if table_name == "maintenance_logs":
            if df.empty: return pd.DataFrame(columns=LOG_COLUMNS)
            for col in LOG_COLUMNS:
                if col not in df.columns: df[col] = ""
            df = df[LOG_COLUMNS]
            for c in ['km_realizada', 'proxima_km', 'valor', 'id']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
        if table_name == "positions" and not df.empty:
            df['odometro'] = pd.to_numeric(df['odometro'], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
        return df
    except:
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

# --- FUNÇÕES DE CADASTRO ---
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
    row = [new_id, placa, tipo, km, str(data), prox, mec, valor, obs, status, resp]
    ws.append_row(row)

def update_maintenance_full(id_m, tipo, km, data, prox, mec, valor, obs, status, resp):
    sh = connect_sheets()
    ws = sh.worksheet("maintenance_logs")
    try:
        cell = ws.find(str(id_m), in_column=1)
        if cell:
            vals = [tipo, km, str(data), prox, mec, valor, obs, status, resp]
            for i, val in enumerate(vals):
                ws.update_cell(cell.row, 3+i, val)
    except: pass

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

# --- SASCAR (INTEGRAÇÃO) ---
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

# --- INTERFACE PRINCIPAL ---
def main():
    init_db()
    
    if 'edit_mode' not in st.session_state: st.session_state.edit_mode = False
    if 'edit_data' not in st.session_state: st.session_state.edit_data = {}
    if 'realizar_id' not in st.session_state: st.session_state.realizar_id = None
    if 'u' not in st.session_state: st.session_state.u = ''
    if 'p' not in st.session_state: st.session_state.p = ''
    if 'last_update' not in st.session_state: st.session_state.last_update = datetime.now() - timedelta(hours=3)

    # Sidebar
    with st.sidebar:
        st.header("Gestão de Frota")
        with st.expander("🔐 Sascar"):
            u = st.text_input("Usuário", value=st.session_state.u)
            p = st.text_input("Senha", type="password", value=st.session_state.p)
            if st.button("Conectar"):
                st.session_state.u = u; st.session_state.p = p
                if baixar_veiculos_auto(u, p): st.success("Conectado!")
                st.rerun()

    # Auto-Update
    agora = datetime.now()
    if st.session_state.u and (agora - st.session_state.last_update).total_seconds() > 3600:
        baixar_posicoes_recentes(st.session_state.u, st.session_state.p)
        st.session_state.last_update = agora
        st.rerun()

    # Título e Botão de Atualizar
    col_a, col_b = st.columns([5,1])
    col_a.title("Painel de Controle")
    if col_b.button("🔄 Sync"):
        if st.session_state.u:
            n = baixar_posicoes_recentes(st.session_state.u, st.session_state.p)
            st.toast(f"Atualizado: {n} posições")
            st.session_state.last_update = agora
            time.sleep(1); st.rerun()
        else: st.warning("Conecte na Sascar")

    # Carregar Dados
    df_v = get_data("vehicles")
    veiculos = df_v['placa'].tolist() if not df_v.empty else []
    df_pos = get_data("positions")
    df_maint = get_data("maintenance_logs")

    # --- SESSÃO DE FORMULÁRIO ---
    with st.expander("➕ Nova Manutenção / Lançamento", expanded=st.session_state.edit_mode):
        d = st.session_state.edit_data if st.session_state.edit_mode else {}
        
        # Seletor de Veículo
        idx_v = 0
        if st.session_state.edit_mode and d.get('placa') in veiculos:
            idx_v = veiculos.index(d.get('placa'))
        
        sel_placa = st.selectbox("Veículo", veiculos, index=idx_v, disabled=st.session_state.edit_mode)
        
        # Sugestão de KM
        km_sugerido = 0.0
        if not df_pos.empty:
            pos = df_pos[df_pos['placa'] == sel_placa]
            if not pos.empty: km_sugerido = float(pos['odometro'].max())

        # Formulário
        with st.form("form_main"):
            c1, c2 = st.columns(2)
            tipos = get_data("service_types")['nome_servico'].tolist() or ["Troca de Óleo"]
            idx_t = tipos.index(d.get('tipo_servico')) if d.get('tipo_servico') in tipos else 0
            
            sel_servico = c1.selectbox("Serviço", tipos, index=idx_t)
            status_ini = d.get('status', 'Agendado')
            is_done = c2.checkbox("Já realizada?", value=(status_ini=='Concluido'))
            
            c3, c4 = st.columns(2)
            val_km = float(d.get('km_realizada', km_sugerido))
            input_km_base = c3.number_input("KM Base (Atual)", value=val_km, step=100.0)
            
            prox_db = float(d.get('proxima_km', 0))
            if st.session_state.edit_mode and prox_db > 0:
                padrao_int = prox_db - input_km_base
            else:
                padrao_int = 10000.0
            
            input_intervalo = c4.number_input("Intervalo (KM)", value=padrao_int, step=1000.0)
            km_final = input_km_base + input_intervalo
            st.info(f"Próxima manutenção prevista para: **{km_final:,.0f} KM**")

            c5, c6, c7 = st.columns(3)
            try: dt_ini = datetime.strptime(str(d.get('data_realizada', '')), '%Y-%m-%d').date()
            except: dt_ini = datetime.now()
            
            input_data = c5.date_input("Data", dt_ini, format="DD/MM/YYYY")
            input_valor = c6.number_input("Valor R$", value=float(d.get('valor', 0)))
            input_resp = c7.text_input("Responsável", value=d.get('responsavel', ''))
            
            obs = st.text_area("Observações", value=d.get('obs', ''))
            
            btn_col1, btn_col2 = st.columns(2)
            if btn_col1.form_submit_button("Salvar Registro"):
                stf = "Concluido" if is_done else "Agendado"
                if st.session_state.edit_mode:
                    update_maintenance_full(d.get('id'), sel_servico, input_km_base, input_data, km_final, "", input_valor, obs, stf, input_resp)
                else:
                    add_maintenance(sel_placa, sel_servico, input_km_base, input_data, km_final, "", input_valor, obs, stf, input_resp)
                
                st.session_state.edit_mode = False
                st.session_state.edit_data = {}
                st.success("Salvo!")
                time.sleep(1); st.rerun()
                
            if st.session_state.edit_mode:
                if btn_col2.form_submit_button("Cancelar Edição"):
                    st.session_state.edit_mode = False
                    st.session_state.edit_data = {}
                    st.rerun()

    # --- MODAL BAIXA ---
    if st.session_state.realizar_id:
        st.write("---")
        st.warning("Finalizando Ordem de Serviço")
        with st.container(border=True):
            item = df_maint[df_maint['id'] == st.session_state.realizar_id].iloc[0]
            with st.form("baixa"):
                st.write(f"**{item['placa']}** - {item['tipo_servico']}")
                cc1, cc2 = st.columns(2)
                dt_fim = cc1.date_input("Data Real", datetime.now())
                val_fim = cc2.number_input("Valor Final", value=float(item['valor']))
                obs_fim = st.text_input("Nota de Fechamento")
                if st.form_submit_button("Confirmar Baixa"):
                    realizar_manutencao(st.session_state.realizar_id, dt_fim, val_fim, obs_fim)
                    st.session_state.realizar_id = None
                    st.rerun()
        if st.button("Cancelar Baixa"):
            st.session_state.realizar_id = None; st.rerun()

    st.write("---")

    # --- VISUALIZAÇÃO (CARDS) ---
    aba1, aba2 = st.tabs(["Aberto", "Histórico"])
    
    # Prepara Dados
    if not df_v.empty and not df_pos.empty:
        last_pos = df_pos.sort_values('timestamp').groupby('placa').tail(1)
        df_view = df_v.merge(last_pos[['placa', 'odometro']], on='placa', how='left')
    else:
        df_view = df_v.copy()
        df_view['odometro'] = 0

    with aba1:
        m_abertas = df_maint[df_maint['status'] != 'Concluido'] if not df_maint.empty and 'status' in df_maint.columns else pd.DataFrame()
        
        if m_abertas.empty:
            st.info("Nenhuma pendência.")
        else:
            km_dict = dict(zip(df_view['placa'], df_view['odometro']))
            
            for _, m in m_abertas.iterrows():
                placa = m['placa']
                km_atual = float(km_dict.get(placa, 0) or 0)
                meta = float(m['proxima_km'] or 0)
                restam = meta - km_atual
                
                # Definição de Cores usando CSS class
                if restam <= 0:
                    css_class = "status-vencido"
                    txt = f"🚨 VENCIDO ({abs(restam):,.0f} km)"
                elif restam < 1000:
                    css_class = "status-atencao"
                    txt = f"⚠️ ATENÇÃO ({restam:,.0f} km)"
                else:
                    css_class = "status-ok"
                    txt = f"🟢 NO PRAZO ({restam:,.0f} km)"
                
                # Card Nativo
                with st.container(border=True):
                    cols = st.columns([3, 2, 1])
                    with cols[0]:
                        st.subheader(placa)
                        st.write(f"**{m['tipo_servico']}**")
                        st.caption(f"Resp: {m.get('responsavel','-')}")
                    with cols[1]:
                        st.markdown(f"<span class='{css_class}'>{txt}</span>", unsafe_allow_html=True)
                        st.write(f"Meta: {meta:,.0f} | Atual: {km_atual:,.0f}")
                        st.write(f"Valor: R$ {m['valor']}")
                    with cols[2]:
                        if st.button("✅", key=f"bx_{m['id']}"): 
                            st.session_state.realizar_id = m['id']; st.rerun()
                        if st.button("✏️", key=f"ed_{m['id']}"):
                            st.session_state.edit_mode = True
                            st.session_state.edit_data = m.to_dict()
                            st.rerun()
                        if st.button("🗑️", key=f"del_{m['id']}"):
                            delete_maintenance(m['id']); st.rerun()

    with aba2:
        m_conc = df_maint[df_maint['status'] == 'Concluido'] if not df_maint.empty and 'status' in df_maint.columns else pd.DataFrame()
        if not m_conc.empty:
            st.dataframe(m_conc, use_container_width=True)
        else:
            st.info("Histórico vazio.")

if __name__ == "__main__":
    main()
