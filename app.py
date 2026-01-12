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

# --- 2. CSS PARA UI MELHORADA ---
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    .status-card { padding: 15px; border-radius: 8px; border: 1px solid #ddd; background: white; margin-bottom: 10px; }
    .status-vencido { color: #d9534f; font-weight: bold; }
    .status-atencao { color: #f0ad4e; font-weight: bold; }
    .status-ok { color: #5cb85c; font-weight: bold; }
    .big-number { font-size: 1.2rem; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- 3. CLASSE DE SERVIÇO SASCAR (API) ---
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

# --- 4. CLASSE DE BANCO DE DADOS (GOOGLE SHEETS) ---
class FleetDatabase:
    def __init__(self, sheet_name="frota_db"):
        self.sheet_name = sheet_name
        # Ordem exata das colunas na planilha:
        # A=id, B=placa, C=tipo, D=km_real, E=data_real, F=prox_km, G=responsavel, H=valor, I=obs, J=status
        self.log_cols = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "responsavel", "valor", "obs", "status"]

    @st.cache_resource
    def _get_connection(_self):
        try:
            creds = st.secrets["gcp_service_account"]
            gc = gspread.service_account_from_dict(creds)
            return gc.open(_self.sheet_name)
        except Exception as e:
            st.error(f"Erro crítico ao conectar no Google Sheets: {e}")
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
                # Garante que todas colunas existam no DF para evitar KeyError
                for col in self.log_cols:
                    if col not in df.columns: df[col] = ""
            return df
        except: return pd.DataFrame()

    def sync_sascar_data(self, sascar_service: SascarService):
        status_msg = st.empty()
        status_msg.info("⏳ Iniciando sincronização com Sascar...")
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
            status_msg.success(f"✅ Sincronizado! {len(novos_dados)} novas posições.")
            time.sleep(2)
            status_msg.empty()
            return True
        status_msg.warning("⚠️ Nenhuma posição nova encontrada.")
        return False

    def add_log(self, data_dict):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
        col_ids = ws.col_values(1)
        next_id = 1
        if len(col_ids) > 1:
            ids = [int(x) for x in col_ids[1:] if str(x).isdigit()]
            if ids: next_id = max(ids) + 1
            
        # --- CORREÇÃO DE ORDEM DAS COLUNAS (G vs K) ---
        # A ordem aqui deve bater EXATAMENTE com self.log_cols
        row = [
            next_id, 
            data_dict['placa'], 
            data_dict['tipo'], 
            data_dict['km'],
            str(data_dict['data']), 
            data_dict['prox_km'], 
            data_dict['resp'],   # <--- Agora na posição correta (Coluna G)
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
                # Ajustando indices baseados na nova estrutura
                # A=1, E=5 (Data), H=8 (Valor), I=9 (Obs), J=10 (Status)
                ws.update_cell(cell.row, 5, str(data_real))
                ws.update_cell(cell.row, 8, valor_final)
                old_obs = ws.cell(cell.row, 9).value
                new_obs = f"{old_obs} | Baixa: {obs_final}" if old_obs else obs_final
                ws.update_cell(cell.row, 9, new_obs)
                ws.update_cell(cell.row, 10, "Concluido")
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")

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
                # Atualiza células específicas
                # B=2(Placa), C=3(Tipo), D=4(KmReal/Base), F=6(Prox), G=7(Resp), H=8(Valor), I=9(Obs)
                ws.update_cell(r, 2, novos_dados['placa'])
                ws.update_cell(r, 3, novos_dados['tipo'])
                ws.update_cell(r, 4, novos_dados['km'])
                ws.update_cell(r, 6, novos_dados['prox_km'])
                ws.update_cell(r, 7, novos_dados['resp'])
                ws.update_cell(r, 8, novos_dados['valor'])
                ws.update_cell(r, 9, novos_dados['obs'])
                return True
        except Exception as e:
            st.error(f"Erro edit: {e}")
            return False

# --- 5. APLICAÇÃO PRINCIPAL ---
def main():
    db = FleetDatabase()
    if 'last_sync' not in st.session_state: st.session_state.last_sync = None
    
    # --- SIDEBAR ---
    with st.sidebar:
        st.header("⚙️ Configurações")
        default_user = st.secrets.get("sascar", {}).get("user", "")
        default_pass = st.secrets.get("sascar", {}).get("password", "")
        with st.expander("Credenciais Sascar", expanded=not default_user):
            sascar_user = st.text_input("Usuário", value=default_user)
            sascar_pass = st.text_input("Senha", type="password", value=default_pass)
        
        if st.button("🔄 Sincronizar Agora"):
            if sascar_user and sascar_pass:
                svc = SascarService(sascar_user, sascar_pass)
                if db.sync_sascar_data(svc):
                    st.session_state.last_sync = datetime.now()
                    st.rerun()

    st.title("🚛 Gestão de Frota")

    # Load Data
    df_v = db.get_dataframe("vehicles")
    df_pos = db.get_dataframe("positions")
    df_logs = db.get_dataframe("maintenance_logs")

    # Processamento Frota
    if not df_pos.empty:
        df_pos['timestamp'] = pd.to_datetime(df_pos['timestamp'], errors='coerce')
        df_pos['odometro'] = pd.to_numeric(df_pos['odometro'], errors='coerce')
        last_pos = df_pos.sort_values('timestamp').groupby('id_veiculo').tail(1)
        if not df_v.empty:
            df_v['id_veiculo'] = df_v['id_veiculo'].astype(str)
            last_pos['id_veiculo'] = last_pos['id_veiculo'].astype(str)
            df_frota = pd.merge(df_v, last_pos[['id_veiculo', 'odometro', 'timestamp']], on='id_veiculo', how='left')
            df_frota['odometro'] = df_frota['odometro'].fillna(0)
        else:
            df_frota = pd.DataFrame(columns=['placa', 'odometro'])
    else:
        df_frota = df_v.copy() if not df_v.empty else pd.DataFrame(columns=['placa'])
        df_frota['odometro'] = 0

    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # --- ABA 1: PENDÊNCIAS ---
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            if not pendentes.empty:
                km_map = dict(zip(df_frota['placa'], df_frota['odometro']))
                
                # Ordenar por urgência (menor km restante primeiro)
                pendentes['km_restante'] = pd.to_numeric(pendentes['proxima_km'], errors='coerce') - pendentes['placa'].map(km_map).fillna(0)
                pendentes = pendentes.sort_values('km_restante')

                for index, row in pendentes.iterrows():
                    placa = row['placa']
                    km_atual = float(km_map.get(placa, 0))
                    meta_km = float(row['proxima_km']) if row['proxima_km'] != '' else 0
                    restante = meta_km - km_atual
                    
                    if restante < 0:
                        status_cls = "status-vencido"; status_txt = f"🚨 VENCIDO ({abs(restante):,.0f} KM)"; border_color = "#d9534f"
                    elif restante < 1000:
                        status_cls = "status-atencao"; status_txt = f"⚠️ ATENÇÃO ({restante:,.0f} KM)"; border_color = "#f0ad4e"
                    else:
                        status_cls = "status-ok"; status_txt = f"🟢 NO PRAZO ({restante:,.0f} KM)"; border_color = "#5cb85c"

                    with st.container():
                        st.markdown(f"""
                        <div class="status-card" style="border-left: 5px solid {border_color}">
                            <div style="display:flex; justify-content:space-between;">
                                <span class="big-number">{placa}</span>
                                <span class="{status_cls}">{status_txt}</span>
                            </div>
                            <div><b>{row['tipo_servico']}</b> | Resp: {row['responsavel']}</div>
                            <div style="color: #666; font-size: 0.9em">Meta: {meta_km:,.0f} km | Atual: {km_atual:,.0f} km</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # --- ACTIONS BAR ---
                        c_baixa, c_edit, c_del = st.columns([2, 2, 1])
                        
                        # 1. BAIXA
                        with c_baixa.expander("✅ Baixar"):
                            with st.form(key=f"bx_{row['id']}"):
                                dt_bx = st.date_input("Data", datetime.now())
                                vl_bx = st.number_input("Valor Final R$", value=float(row['valor']) if row['valor'] else 0.0)
                                obs_bx = st.text_input("Obs Fechamento")
                                if st.form_submit_button("Concluir"):
                                    db.update_log_status(row['id'], dt_bx, vl_bx, obs_bx)
                                    st.success("Baixado!"); time.sleep(1); st.rerun()

                        # 2. EDITAR (NOVA FUNCIONALIDADE)
                        with c_edit.expander("✏️ Editar"):
                            with st.form(key=f"ed_{row['id']}"):
                                ed_tipo = st.text_input("Serviço", value=row['tipo_servico'])
                                ed_resp = st.text_input("Responsável", value=row['responsavel'])
                                ed_km_base = st.number_input("KM Base", value=float(row['km_realizada']) if row['km_realizada'] else 0.0)
                                ed_prox = st.number_input("Próxima KM (Meta)", value=float(row['proxima_km']) if row['proxima_km'] else 0.0)
                                ed_valor = st.number_input("Valor Prev.", value=float(row['valor']) if row['valor'] else 0.0)
                                ed_obs = st.text_area("Obs", value=row['obs'])
                                if st.form_submit_button("Salvar Edição"):
                                    novos = {'placa': row['placa'], 'tipo': ed_tipo, 'resp': ed_resp, 
                                             'km': ed_km_base, 'prox_km': ed_prox, 'valor': ed_valor, 'obs': ed_obs}
                                    if db.edit_log_full(row['id'], novos):
                                        st.success("Editado!"); time.sleep(1); st.rerun()

                        # 3. EXCLUIR (NOVA FUNCIONALIDADE)
                        if c_del.button("🗑️", key=f"del_{row['id']}", help="Excluir permanentemente"):
                            if db.delete_log(row['id']):
                                st.toast("Item excluído!"); time.sleep(1); st.rerun()
            else:
                st.info("Nenhuma manutenção pendente.")

    # --- ABA 2: NOVO LANÇAMENTO ---
    with tab_novo:
        st.subheader("Registrar Manutenção")
        
        # Keys para controlar a limpeza do formulário
        keys_to_clear = ["n_placa", "n_serv", "n_km", "n_inter", "n_dt", "n_val", "n_resp", "n_obs", "n_done", "n_agendar"]

        with st.form("form_novo_registro", clear_on_submit=False):
            lista_placas = df_frota['placa'].unique().tolist()
            c1, c2 = st.columns(2)
            sel_placa = c1.selectbox("Placa", lista_placas, key="n_placa")
            sel_servico = c2.selectbox("Serviço", ["Troca de Óleo", "Pneus", "Freios", "Correia", "Filtros", "Suspensão", "Elétrica", "Outros"], key="n_serv")
            
            # KM Sugerido
            km_sugerido = 0.0
            if sel_placa:
                k = df_frota.loc[df_frota['placa'] == sel_placa, 'odometro']
                if not k.empty: km_sugerido = float(k.values[0])
            
            c3, c4 = st.columns(2)
            # KM Base sempre editável
            km_base = c3.number_input("KM na data do serviço (Atual)", value=km_sugerido, step=100.0, key="n_km")
            intervalo = c4.number_input("Intervalo para próxima (KM)", value=10000.0, step=1000.0, key="n_inter")
            
            proxima_meta = km_base + intervalo
            st.caption(f"📅 Próxima manutenção programada para: **{proxima_meta:,.0f} KM**")

            c5, c6, c7 = st.columns(3)
            dt_reg = c5.date_input("Data do serviço", datetime.now(), key="n_dt")
            valor_prev = c6.number_input("Valor (R$)", value=0.0, key="n_val")
            resp = c7.text_input("Responsável / Mecânico", key="n_resp")
            
            obs = st.text_area("Observações", key="n_obs")
            
            st.divider()
            cc1, cc2 = st.columns(2)
            ja_feito = cc1.checkbox("✅ Já realizada (Histórico)", value=True, key="n_done")
            agendar_prox = cc2.checkbox("🔄 Criar próxima pendência?", value=True, key="n_agendar")
            
            if st.form_submit_button("💾 Salvar Registro"):
                status_atual = "Concluido" if ja_feito else "Agendado"
                km_realizada_log = km_base if ja_feito else ""
                
                # 1. Registro Principal
                dados_originais = {
                    "placa": sel_placa, "tipo": sel_servico, "km": km_realizada_log,
                    "data": dt_reg, "prox_km": proxima_meta, "valor": valor_prev, 
                    "obs": obs, "resp": resp, "status": status_atual
                }
                db.add_log(dados_originais)
                
                # 2. Registro Recorrente
                if ja_feito and agendar_prox:
                    dados_futuros = {
                        "placa": sel_placa, "tipo": sel_servico, "km": "", "data": "",
                        "prox_km": proxima_meta, "valor": 0, "obs": "Agendamento automático.",
                        "resp": "", "status": "Agendado"
                    }
                    db.add_log(dados_futuros)
                
                st.success("Salvo com sucesso!")
                
                # Limpa os campos da session state manualmente
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                
                time.sleep(1)
                st.rerun()

    # --- ABA 3: HISTÓRICO ---
    with tab_hist:
        if not df_logs.empty and 'status' in df_logs.columns:
            concluidos = df_logs[df_logs['status'] == 'Concluido'].sort_values('id', ascending=False)
            st.dataframe(concluidos, use_container_width=True, hide_index=True)
        else:
            st.write("Sem histórico.")

if __name__ == "__main__":
    main()
