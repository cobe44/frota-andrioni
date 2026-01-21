import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from datetime import datetime, timedelta
import time
import os
import json
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
try:
    import tomllib
except ImportError:
    import tomli as tomllib

def load_config():
    try:
        with open("config.toml", "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        # Fallback para st.secrets ou env se o arquivo não existir ou falhar
        print(f"❌ [DEBUG] Error loading config.toml: {e}")
        return {}

CONFIG = load_config()

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Gestão de Frota | Andrioni",
    layout="wide",
    page_icon="🚛"
)

# --- 2. CSS ---
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
        height: 100%;
    }
    .status-vencido { color: #d9534f; font-weight: 700; text-transform: uppercase; font-size: 0.85rem; }
    .status-atencao { color: #f0ad4e; font-weight: 700; text-transform: uppercase; font-size: 0.85rem; }
    .status-ok { color: #5cb85c; font-weight: 700; text-transform: uppercase; font-size: 0.85rem; }
    .placa-title { font-size: 1.1rem; font-weight: 700; color: #333; }
    .meta-info { color: #666; font-size: 0.80rem; margin-top: 4px; }
    .block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)

# --- 3. BANCO DE DADOS ---
class FleetDatabase:
    def __init__(self, sheet_name="frota_db"):
        self.sheet_name = sheet_name
        self.log_cols = ["id", "placa", "tipo_servico", "km_realizada", "data_realizada", "proxima_km", "responsavel", "valor", "obs", "status"]

    def _get_pg_engine(self):
        try:
            db_url = None
            
            # 1. Tenta pegar dos st.secrets (Produção/Streamlit Cloud)
            try:
                if "database" in st.secrets and "url" in st.secrets["database"]:
                    db_url = st.secrets["database"]["url"]
            except FileNotFoundError:
                pass 
            except Exception:
                pass
            
            # 2. Se não achou, tenta do config.toml (Local)
            if not db_url:
                db_url = CONFIG.get("database", {}).get("url")
            
            # 3. Fallback para variáveis de ambiente
            if not db_url:
                db_url = os.getenv("DATABASE_URL")
            
            if not db_url:
                print("❌ [DEBUG] DATABASE_URL não encontrada (secrets, config ou env)!")
                return None

            # SQLAlchemy Engine
            return create_engine(db_url)
        except Exception as e:
            st.error(f"Erro ao criar engine Postgres: {e}")
            return None

    @st.cache_resource
    def _get_connection(_self):
        creds_dict = None
        # 1. Tenta pegar dos st.secrets (Streamlit Cloud)
        try:
            if "gcp_service_account" in st.secrets:
                creds_dict = st.secrets["gcp_service_account"]
        except: pass
        
        # 2. Fallback: Variável de Ambiente GCP_CREDENTIALS (String JSON)
        if not creds_dict:
            env_json = os.getenv("GCP_CREDENTIALS")
            if env_json:
                try:
                    creds_dict = json.loads(env_json)
                except Exception as e:
                    st.error(f"Erro ao fazer parse do JSON em GCP_CREDENTIALS: {e}")
            
            # 3. Fallback: Variáveis de Ambiente individuais (Formato atual do user)
            elif os.getenv("private_key"): 
                try:
                    # Reconstrói o dicionário a partir das variáveis soltas no .env
                    creds_dict = {
                        "type": os.getenv("type"),
                        "project_id": os.getenv("project_id"),
                        "private_key_id": os.getenv("private_key_id"),
                        "private_key": os.getenv("private_key").replace("\\n", "\n") if os.getenv("private_key") else None,
                        "client_email": os.getenv("client_email"),
                        "client_id": os.getenv("client_id"),
                        "auth_uri": os.getenv("auth_uri"),
                        "token_uri": os.getenv("token_uri"),
                        "auth_provider_x509_cert_url": os.getenv("auth_provider_x509_cert_url"),
                        "client_x509_cert_url": os.getenv("client_x509_cert_url"),
                        "universe_domain": os.getenv("universe_domain")
                    }
                except Exception as e:
                     st.error(f"Erro ao montar credenciais via variáveis soltas: {e}")

        if not creds_dict:
            st.error("Credenciais do Google Sheets não encontradas (st.secrets ou env GCP_CREDENTIALS).")
            return None

        try:
            gc = gspread.service_account_from_dict(creds_dict)
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
    
    def get_dataframe(self, worksheet_name):
        # Rota para Postgres (Dados de Rastreamento)
        if worksheet_name == "vehicles":
            engine = self._get_pg_engine()
            if not engine: return pd.DataFrame()
            try:
                # Query com JOIN para pegar o último odômetro
                query = """
                    SELECT v.id_sascar as id_veiculo, v.placa, p.odometro 
                    FROM veiculos v
                    LEFT JOIN (
                        SELECT DISTINCT ON (id_veiculo) id_veiculo, odometro
                        FROM posicoes_raw
                        ORDER BY id_veiculo, data_hora DESC
                    ) p ON v.id_sascar = p.id_veiculo
                """
                with engine.connect() as conn:
                    df = pd.read_sql_query(query, conn)
                return df
            except Exception as e:
                st.error(f"Erro ao ler veículos do DB: {e}")
                return pd.DataFrame()

        if worksheet_name == "positions":
            engine = self._get_pg_engine()
            if not engine: return pd.DataFrame()
            try:
                query_opt = """
                    SELECT DISTINCT ON (id_veiculo) 
                        id_veiculo, 
                        data_hora as timestamp, 
                        odometro, 
                        latitude, 
                        longitude 
                    FROM posicoes_raw 
                    ORDER BY id_veiculo, data_hora DESC
                """
                with engine.connect() as conn:
                    df = pd.read_sql_query(query_opt, conn)
                return df
            except Exception as e:
                st.error(f"Erro ao ler posições do DB: {e}")
                return pd.DataFrame()

        # Rota Original (Google Sheets)
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
        defaults = ["Troca de Óleo Motor", "Troca de Óleo Cambio e Diferencial", "Pneus", "Freios", "Correia", "Filtros", "Suspensão", "Elétrica", "Outros"]
        sh = self._get_connection()
        if not sh: return defaults
        try:
            ws = self._safe_get_worksheet(sh, "service_types")
            if not ws: return defaults
            vals = ws.col_values(2) 
            if len(vals) > 1: return vals[1:]
            return defaults
        except: return defaults

    def update_manual_km(self, placa, novo_km):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "veiculos_manuais")
        if not ws: return False
        try:
            cell = ws.find(placa, in_column=1)
            if cell:
                ws.update_cell(cell.row, 2, novo_km)
            else:
                ws.append_row([placa, novo_km])
            carregar_dados_gerais.clear()
            return True
        except: return False

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
        carregar_dados_gerais.clear()

    def update_log_status(self, log_id, data_real, valor_final, obs_final, resp_final, km_final):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                ws.update_cell(cell.row, 4, km_final)
                ws.update_cell(cell.row, 5, str(data_real))
                ws.update_cell(cell.row, 7, resp_final)
                ws.update_cell(cell.row, 8, valor_final)
                old_obs = ws.cell(cell.row, 9).value
                new_obs = f"{old_obs} | Baixa: {obs_final}" if old_obs else obs_final
                ws.update_cell(cell.row, 9, new_obs)
                ws.update_cell(cell.row, 10, "Concluido")
                carregar_dados_gerais.clear()
        except: pass

    def delete_log(self, log_id):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return False
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell: 
                ws.delete_rows(cell.row)
                carregar_dados_gerais.clear()
                return True
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
                carregar_dados_gerais.clear()
                return True
        except: return False

# --- CACHE ---
@st.cache_data(ttl=300)
def carregar_dados_gerais():
    db_temp = FleetDatabase()
    return (
        db_temp.get_dataframe("vehicles"),
        db_temp.get_dataframe("positions"),
        db_temp.get_dataframe("veiculos_manuais"),
        db_temp.get_dataframe("maintenance_logs"),
        db_temp.get_services_list()
    )

# --- 4. APP PRINCIPAL ---
def main():
    db = FleetDatabase()
    
    with st.sidebar:
        st.header("Gestão de Frota")
        st.caption("🔄 Sincronização automática via GitHub")
        
        if st.button("Atualizar Tela (F5)", use_container_width=True):
            carregar_dados_gerais.clear()
            st.rerun()
            
        st.divider()
        
        df_v_sascar, df_pos_sascar, df_v_manual, df_logs, lista_servicos_db = carregar_dados_gerais()

        with st.expander("🚗 Atualizar KM Manual", expanded=True):
            lista_manuais = df_v_manual['placa'].tolist() if not df_v_manual.empty else []
            placa_manual = st.selectbox("Selecione Manual", lista_manuais)
            if placa_manual:
                km_atual_manual = 0.0
                linha_atual = df_v_manual[df_v_manual['placa'] == placa_manual]
                if not linha_atual.empty:
                    km_atual_manual = float(linha_atual.iloc[0]['odometro'])
                # value=None deixa vazio, placeholder mostra o valor atual como dica
                novo_km_manual = st.number_input("Novo KM", value=None, placeholder=f"Atual: {km_atual_manual}", step=100.0)
                if st.button("Salvar KM"):
                    if novo_km_manual is None:
                        st.error("Digite o valor.")
                    elif db.update_manual_km(placa_manual, novo_km_manual):
                        st.success("Atualizado!")
                        time.sleep(1); st.rerun()
            
            with st.popover("➕ Cadastrar Novo Manual"):
                novo_placa = st.text_input("Nova Placa")
                novo_km_ini = st.number_input("KM Inicial", value=0.0)
                if st.button("Criar"):
                    if db.update_manual_km(novo_placa, novo_km_ini):
                        st.success("Criado!"); time.sleep(1); st.rerun()

    st.title("🚛 Painel de Controle")

    # --- PROCESSAMENTO ---
    mapa_km_total = {}

    # --- DEBUG INCONDICIONAL ---
    with st.sidebar:
        st.divider()
        st.write("🐞 DEBUG CHECK")
        
        # Testar conexão diretamente para exibir erro se houver
        # db._get_pg_engine() retorna um engine, não uma conexão raw
        engine_test = db._get_pg_engine()
        if engine_test is None:
            st.error("❌ FALHA CONEXÃO POSTGRES! Verifique logs/console.")
        else:
            try:
                with engine_test.connect() as conn:
                    st.success("✅ Conexão DB OK")
            except Exception as e:
                st.error(f"❌ Erro ao conectar: {e}")

        st.write(f"Veículos (DB): {len(df_v_sascar)}")
        st.write(f"Posições (DB): {len(df_pos_sascar)}")
        st.write(f"Manuais: {len(df_v_manual)}")
    # ---------------------------

    if not df_v_sascar.empty:
        # Agora o odômetro já vem na query de veículos (join)
        for _, row in df_v_sascar.iterrows():
            if pd.notna(row.get('odometro')):
                mapa_km_total[row['placa']] = row['odometro']
    
    # Manuais sobrescrevem automaticos
    if not df_v_manual.empty:
        for _, row in df_v_manual.iterrows():
             mapa_km_total[row['placa']] = float(row['odometro'])

    todas_placas = list(mapa_km_total.keys())
    todas_placas.sort()

    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # --- ABA 1: PENDÊNCIAS (GRID) ---
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            if not pendentes.empty:
                pendentes['km_restante'] = pd.to_numeric(pendentes['proxima_km'], errors='coerce') - pendentes['placa'].map(mapa_km_total).fillna(0)
                pendentes = pendentes.sort_values('km_restante')

                # GRID 3 POR LINHA
                cols_num = 3
                rows = [pendentes.iloc[i:i+cols_num] for i in range(0, len(pendentes), cols_num)]

                for row_chunk in rows:
                    cols = st.columns(cols_num)
                    for idx, (index, row) in enumerate(row_chunk.iterrows()):
                        with cols[idx]: 
                            placa = row['placa']
                            km_atual = float(mapa_km_total.get(placa, 0))
                            meta_km = float(row['proxima_km']) if row['proxima_km'] != '' else 0
                            restante = meta_km - km_atual
                            
                            if restante < 0:
                                s_cls = "status-vencido"; s_txt = f"🚨 VENCIDO ({abs(restante):,.0f} KM)"; b_col = "#d9534f"
                            elif restante < 3000:
                                s_cls = "status-atencao"; s_txt = f"⚠️ ATENÇÃO ({restante:,.0f} KM)"; b_col = "#f0ad4e"
                            else:
                                s_cls = "status-ok"; s_txt = f"🟢 NO PRAZO ({restante:,.0f} KM)"; b_col = "#5cb85c"

                            st.markdown(f"""
                            <div class="status-card" style="border-left: 5px solid {b_col}">
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <span class="placa-title">{placa}</span>
                                    <span class="{s_cls}">{s_txt}</span>
                                </div>
                                <div style="margin: 5px 0;"><b>{row['tipo_servico']}</b></div>
                                <div class="meta-info">Resp: {row['responsavel']}</div>
                                <div class="meta-info">Meta: {meta_km:,.0f} | Atual: {km_atual:,.0f}</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # BOX DE BAIXA LIMPO
                            with st.expander("✅ Baixar O.S."):
                                with st.form(key=f"bx_{row['id']}"):
                                    # value=None deixa o campo VAZIO
                                    km_real_bx = st.number_input("KM Realizado (Painel)", value=None, placeholder="Digite o KM...", step=100.0)
                                    dt_bx = st.date_input("Data Real", datetime.now() - timedelta(hours=3))
                                    vl_bx = st.number_input("Valor R$", value=None, placeholder="0.00", step=10.0)
                                    resp_bx = st.text_input("Responsável", value=row['responsavel']) 
                                    obs_bx = st.text_input("Obs")
                                    
                                    st.divider()
                                    reagendar_bx = st.checkbox(f"🔄 Reagendar próxima?")
                                    
                                    intervalo_final = None
                                    if reagendar_bx:
                                        intervalo_final = st.number_input("Intervalo (KM)", value=None, placeholder="Digite o intervalo...", step=1000.0)
                                    
                                    if st.form_submit_button("Concluir"):
                                        if km_real_bx is None:
                                            st.error("Digite o KM Realizado!")
                                        else:
                                            # Se valor for None vira 0.0 para salvar
                                            val_save = vl_bx if vl_bx is not None else 0.0
                                            db.update_log_status(row['id'], dt_bx, val_save, obs_bx, resp_bx, km_real_bx)
                                            
                                            if reagendar_bx:
                                                if intervalo_final is None:
                                                    st.warning("Intervalo não preenchido. Agendamento ignorado.")
                                                else:
                                                    nova_meta = km_real_bx + intervalo_final
                                                    dados_reagendamento = {
                                                        "placa": placa, "tipo": row['tipo_servico'], "km": "", "data": "",
                                                        "prox_km": nova_meta, "valor": 0, "obs": "Reagendamento automático na baixa.",
                                                        "resp": resp_bx, "status": "Agendado"
                                                    }
                                                    db.add_log(dados_reagendamento)
                                                    st.toast(f"✅ Baixado e reagendado para {nova_meta:,.0f} KM!")
                                            else:
                                                st.toast("✅ O.S. Baixada com sucesso!")
                                            time.sleep(1); st.rerun()

                            with st.expander("✏️ Editar"):
                                with st.form(key=f"ed_{row['id']}"):
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
                            
                            if st.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                                db.delete_log(row['id']); st.rerun()
            else:
                st.info("Nenhuma pendência.")

    # --- ABA 2: NOVO LANÇAMENTO (LIMPO) ---
    with tab_novo:
        st.subheader("Registrar Manutenção")
        c_sel1, c_sel2 = st.columns(2)
        p_selected = c_sel1.selectbox("Selecione a Placa", todas_placas) if todas_placas else None
        s_selected = c_sel2.selectbox("Selecione o Serviço", lista_servicos_db)
        
        st.divider()
        with st.form("form_novo", clear_on_submit=False):
            c1, c2 = st.columns(2)
            # CAMPOS LIMPOS (None)
            km_base = c1.number_input("KM Atual (Base)", value=None, placeholder="Digite o KM...", step=100.0)
            intervalo = c2.number_input("Intervalo (KM)", value=None, placeholder="Digite o intervalo...", step=1000.0)
            
            c3, c4, c5 = st.columns(3)
            dt_reg = c3.date_input("Data do Serviço", datetime.now() - timedelta(hours=3))
            val_reg = c4.number_input("Valor (R$)", value=None, placeholder="0.00", step=10.0)
            resp_reg = c5.text_input("Responsável")
            obs_reg = st.text_area("Observações")
            
            st.divider()
            cc1, cc2 = st.columns(2)
            is_done = cc1.checkbox("✅ Já realizada (Salvar no Histórico)", value=True)
            do_sched = cc2.checkbox("🔄 Agendar a próxima?", value=True)
            
            if st.form_submit_button("💾 Salvar Lançamento"):
                if not p_selected:
                    st.error("Selecione uma placa primeiro.")
                elif km_base is None:
                    st.error("Preencha o KM Base!")
                elif intervalo is None:
                    st.error("Preencha o Intervalo!")
                else:
                    prox_calc = km_base + intervalo
                    val_save = val_reg if val_reg is not None else 0.0
                    
                    stt = "Concluido" if is_done else "Agendado"
                    km_log = km_base if is_done else ""
                    d1 = {"placa": p_selected, "tipo": s_selected, "km": km_log, "data": dt_reg, 
                          "prox_km": prox_calc, "valor": val_save, "obs": obs_reg, "resp": resp_reg, "status": stt}
                    db.add_log(d1)
                    if is_done and do_sched:
                        d2 = {"placa": p_selected, "tipo": s_selected, "km": "", "data": "", 
                              "prox_km": prox_calc, "valor": 0, "obs": "Agendamento automático.", 
                              "resp": "", "status": "Agendado"}
                        db.add_log(d2)
                    st.toast("Salvo com sucesso!")
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
