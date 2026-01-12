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
    """Gerencia a comunicação com a Sascar para não sujar o código principal."""
    
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
    """Gerencia leitura e escrita no Sheets com Cache."""
    
    def __init__(self, sheet_name="frota_db"):
        self.sheet_name = sheet_name
        self.log_cols = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "mecanico", "valor", "obs", "status", "responsavel"]

    @st.cache_resource
    def _get_connection(_self):
        # O uso de _self e cache_resource impede reconexão constante
        try:
            creds = st.secrets["gcp_service_account"]
            gc = gspread.service_account_from_dict(creds)
            return gc.open(_self.sheet_name)
        except Exception as e:
            st.error(f"Erro crítico ao conectar no Google Sheets: {e}")
            return None

    def init_tables(self):
        sh = self._get_connection()
        if not sh: return
        existing = [ws.title for ws in sh.worksheets()]
        
        if "maintenance_logs" not in existing:
            ws = sh.add_worksheet("maintenance_logs", 100, 20)
            ws.append_row(self.log_cols)
        if "vehicles" not in existing:
            ws = sh.add_worksheet("vehicles", 100, 5)
            ws.append_row(["id_veiculo", "placa"])
        if "positions" not in existing:
            ws = sh.add_worksheet("positions", 100, 5)
            ws.append_row(["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])

    def get_dataframe(self, worksheet_name):
        sh = self._get_connection()
        if not sh: return pd.DataFrame()
        try:
            ws = sh.worksheet(worksheet_name)
            data = ws.get_all_records()
            return pd.DataFrame(data)
        except: return pd.DataFrame()

    def sync_sascar_data(self, sascar_service: SascarService):
        """Lógica inteligente de sincronização."""
        status_msg = st.empty()
        status_msg.info("⏳ Iniciando sincronização com Sascar...")
        
        # 1. Atualizar Veículos (Se necessário)
        veiculos_api = sascar_service.get_vehicles()
        if veiculos_api:
            sh = self._get_connection()
            ws = sh.worksheet("vehicles")
            ws.clear()
            ws.append_row(["id_veiculo", "placa"])
            ws.append_rows(veiculos_api)
        
        # 2. Atualizar Posições
        pos_api = sascar_service.get_positions(qtd=500)
        if pos_api:
            # Mapear placas
            df_v = self.get_dataframe("vehicles")
            map_placa = dict(zip(df_v['id_veiculo'].astype(str), df_v['placa']))
            
            novos_dados = []
            for p in pos_api:
                placa = map_placa.get(str(p['id_veiculo']), "Desconhecido")
                novos_dados.append([
                    p['id_pacote'], p['id_veiculo'], placa, p['timestamp'], p['odometro']
                ])
            
            # Salvar append otimizado
            sh = self._get_connection()
            ws_pos = sh.worksheet("positions")
            ws_pos.append_rows(novos_dados)
            
            # Limpeza (manter apenas ultimas 48h ou last unique) - Simplificado para append
            status_msg.success(f"✅ Sincronizado! {len(novos_dados)} novas posições.")
            time.sleep(2)
            status_msg.empty()
            return True
        
        status_msg.warning("⚠️ Nenhuma posição nova encontrada ou erro na API.")
        return False

    def add_log(self, data_dict):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
        
        # Gerar ID
        col_ids = ws.col_values(1)
        next_id = 1
        if len(col_ids) > 1:
            ids = [int(x) for x in col_ids[1:] if str(x).isdigit()]
            if ids: next_id = max(ids) + 1
            
        row = [
            next_id, data_dict['placa'], data_dict['tipo'], data_dict['km'],
            str(data_dict['data']), data_dict['prox_km'], "", 
            data_dict['valor'], data_dict['obs'], data_dict['status'], data_dict['resp']
        ]
        ws.append_row(row)

    def update_log_status(self, log_id, data_real, valor_final, obs_final):
        sh = self._get_connection()
        ws = sh.worksheet("maintenance_logs")
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                # Colunas: 5=DataReal, 8=Valor, 9=Obs, 10=Status (ajustar conforme indice 1-based)
                ws.update_cell(cell.row, 5, str(data_real))
                ws.update_cell(cell.row, 8, valor_final)
                
                old_obs = ws.cell(cell.row, 9).value
                new_obs = f"{old_obs} | Baixa: {obs_final}" if old_obs else obs_final
                ws.update_cell(cell.row, 9, new_obs)
                ws.update_cell(cell.row, 10, "Concluido")
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")

# --- 5. APLICAÇÃO PRINCIPAL ---
def main():
    # Inicializa Classes
    db = FleetDatabase()
    
    # Session State para controle de fluxo
    if 'last_sync' not in st.session_state: st.session_state.last_sync = None
    
    # --- SIDEBAR (CONFIGURAÇÃO) ---
    with st.sidebar:
        st.header("⚙️ Configurações")
        
        # Tenta pegar credenciais dos secrets, senão pede input (SÓ UMA VEZ)
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
            else:
                st.error("Preencha as credenciais.")

        if st.session_state.last_sync:
            st.caption(f"Última atualização: {st.session_state.last_sync.strftime('%H:%M')}")

    # --- CORPO PRINCIPAL ---
    st.title("🚛 Gestão de Frota")

    # Carregamento de Dados (Cacheado pelo Streamlit automaticamente se nada mudou)
    df_v = db.get_dataframe("vehicles")
    df_pos = db.get_dataframe("positions")
    df_logs = db.get_dataframe("maintenance_logs")

    # Tratamento de dados para visualização
    if not df_pos.empty:
        # Pega a ultima posicao de cada carro
        df_pos['timestamp'] = pd.to_datetime(df_pos['timestamp'], errors='coerce')
        df_pos['odometro'] = pd.to_numeric(df_pos['odometro'], errors='coerce')
        last_pos = df_pos.sort_values('timestamp').groupby('id_veiculo').tail(1)
        
        # Junta com veículos
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

    # --- TABS ---
    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # ABA 1: PENDÊNCIAS (Dashboard)
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            
            if not pendentes.empty:
                # Criar dicionário de KM atual para cálculo rápido
                km_map = dict(zip(df_frota['placa'], df_frota['odometro']))
                
                col1, col2 = st.columns(2) # Grid layout
                for index, row in pendentes.iterrows():
                    placa = row['placa']
                    km_atual = float(km_map.get(placa, 0))
                    meta_km = float(row['proxima_km']) if row['proxima_km'] != '' else 0
                    restante = meta_km - km_atual
                    
                    # Definição de estilo
                    if restante < 0:
                        status_cls = "status-vencido"
                        status_txt = f"🚨 VENCIDO HÁ {abs(restante):,.0f} KM"
                        border_color = "red"
                    elif restante < 1000:
                        status_cls = "status-atencao"
                        status_txt = f"⚠️ TROCAR EM {restante:,.0f} KM"
                        border_color = "orange"
                    else:
                        status_cls = "status-ok"
                        status_txt = f"🟢 OK ({restante:,.0f} KM restantes)"
                        border_color = "green"

                    with st.container():
                        st.markdown(f"""
                        <div class="status-card" style="border-left: 5px solid {border_color}">
                            <div style="display:flex; justify-content:space-between;">
                                <span class="big-number">{placa}</span>
                                <span class="{status_cls}">{status_txt}</span>
                            </div>
                            <div><b>{row['tipo_servico']}</b> | Resp: {row['responsavel']}</div>
                            <div style="color: #666; font-size: 0.9em">
                                Atual: {km_atual:,.0f} km | Meta: {meta_km:,.0f} km
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Botão de Baixa (Expander para não poluir)
                        with st.expander(f"Baixar O.S. #{row['id']}"):
                            with st.form(key=f"form_baixa_{row['id']}"):
                                c_d, c_v = st.columns(2)
                                dt_bx = c_d.date_input("Data Real", datetime.now())
                                vl_bx = c_v.number_input("Valor Final (R$)", value=float(row['valor']) if row['valor'] else 0.0)
                                obs_bx = st.text_input("Observação de Fechamento")
                                if st.form_submit_button("✅ Concluir Manutenção"):
                                    db.update_log_status(row['id'], dt_bx, vl_bx, obs_bx)
                                    st.success("Baixa realizada!")
                                    time.sleep(1)
                                    st.rerun()
            else:
                st.info("Nenhuma manutenção pendente! 🎉")
        else:
            st.info("Banco de dados de manutenções vazio.")

    # ABA 2: NOVO LANÇAMENTO (Lógica de Recorrência Adicionada)
    
    with tab_novo:
        st.subheader("Agendar ou Registrar Manutenção")
        with st.form("form_novo_registro"):
            lista_placas = df_frota['placa'].unique().tolist()
            c1, c2 = st.columns(2)
            sel_placa = c1.selectbox("Placa", lista_placas)
            
            # Adicione seus serviços aqui
            lista_servicos = ["Troca de Óleo", "Pneus", "Freios", "Correia", "Filtros", "Suspensão", "Elétrica", "Outros"]
            sel_servico = c2.selectbox("Serviço", lista_servicos)
            
            # Tenta pegar KM atual do veículo selecionado
            km_sugerido = 0.0
            if sel_placa:
                k = df_frota.loc[df_frota['placa'] == sel_placa, 'odometro']
                if not k.empty: km_sugerido = float(k.values[0])
            
            c3, c4 = st.columns(2)
            # KM Base: Se for algo já realizado, é o KM que estava no painel na hora da manutenção
            km_base = c3.number_input("KM na data do serviço (Atual)", value=km_sugerido, step=100.0)
            intervalo = c4.number_input("Intervalo para a próxima (KM)", value=10000.0, step=1000.0)
            
            # Cálculo visual da próxima
            proxima_meta = km_base + intervalo
            st.caption(f"📅 A próxima manutenção será programada para: **{proxima_meta:,.0f} KM**")

            c5, c6, c7 = st.columns(3)
            dt_reg = c5.date_input("Data do serviço", datetime.now())
            valor_prev = c6.number_input("Valor (R$)", value=0.0)
            resp = c7.text_input("Responsável / Mecânico")
            
            obs = st.text_area("Observações")
            
            st.divider()
            
            # --- LÓGICA DE RECORRÊNCIA ---
            col_check1, col_check2 = st.columns(2)
            ja_feito = col_check1.checkbox("✅ Já realizada (Salvar no Histórico)", value=True)
            # Este checkbox só aparece/faz sentido se a manutenção já foi feita
            agendar_prox = col_check2.checkbox("🔄 Criar pendência da próxima automaticamente?", value=True, disabled=not ja_feito)
            
            if st.form_submit_button("💾 Salvar Registro"):
                # 1. Salva o registro que você acabou de preencher
                status_atual = "Concluido" if ja_feito else "Agendado"
                
                # Se já foi feito, usamos o KM Base como 'km_realizada'. Se é agendado, deixamos 0 ou vazio.
                km_realizada_log = km_base if ja_feito else ""
                
                dados_originais = {
                    "placa": sel_placa, 
                    "tipo": sel_servico, 
                    "km": km_realizada_log, # KM que foi feito
                    "data": dt_reg, 
                    "prox_km": proxima_meta, # A meta dessa manutenção (se for agendada) ou referência
                    "valor": valor_prev, 
                    "obs": obs, 
                    "resp": resp,
                    "status": status_atual
                }
                db.add_log(dados_originais)
                
                # 2. SE estiver marcado "Já feita" E "Agendar próxima", cria o SEGUNDO registro
                if ja_feito and agendar_prox:
                    dados_futuros = {
                        "placa": sel_placa,
                        "tipo": sel_servico,
                        "km": "", # Ainda não foi realizada
                        "data": "", # Data futura indefinida
                        "prox_km": proxima_meta, # A meta é o cálculo feito acima (Base + Intervalo)
                        "valor": 0, # Valor futuro desconhecido
                        "obs": "Agendamento automático gerado após baixa.",
                        "resp": "",
                        "status": "Agendado"
                    }
                    db.add_log(dados_futuros)
                    st.toast("Foram criados 2 registros: Histórico + Próxima Pendência.")
                else:
                    st.toast("Registro salvo.")

                time.sleep(1.5)
                st.rerun()

    # ABA 3: HISTÓRICO
    with tab_hist:
        if not df_logs.empty:
            concluidos = df_logs[df_logs['status'] == 'Concluido']
            st.dataframe(concluidos, use_container_width=True, hide_index=True)
        else:
            st.write("Sem histórico.")

if __name__ == "__main__":
    main()

