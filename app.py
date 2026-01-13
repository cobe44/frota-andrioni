import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from datetime import datetime, timedelta
import time

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
    }
    .status-vencido { color: #d9534f; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-atencao { color: #f0ad4e; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .status-ok { color: #5cb85c; font-weight: 700; text-transform: uppercase; font-size: 0.9rem; }
    .placa-title { font-size: 1.1rem; font-weight: 700; color: #333; }
    .meta-info { color: #666; font-size: 0.85rem; margin-top: 4px; }
    .block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)

# --- 3. BANCO DE DADOS (GOOGLE SHEETS) ---
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
            return True
        except: return False

    # --- FUNÇÕES DE LOG (CRUD) ---
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

    # --- ATUALIZADO: SALVA KM REALIZADA ---
    def update_log_status(self, log_id, data_real, valor_final, obs_final, resp_final, km_final):
        sh = self._get_connection()
        ws = self._safe_get_worksheet(sh, "maintenance_logs")
        if not ws: return
        try:
            cell = ws.find(str(log_id), in_column=1)
            if cell:
                ws.update_cell(cell.row, 4, km_final)       # KM Realizada (Coluna D)
                ws.update_cell(cell.row, 5, str(data_real)) # Data Realizada
                ws.update_cell(cell.row, 7, resp_final)     # Responsável
                ws.update_cell(cell.row, 8, valor_final)    # Valor
                
                old_obs = ws.cell(cell.row, 9).value
                new_obs = f"{old_obs} | Baixa: {obs_final}" if old_obs else obs_final
                ws.update_cell(cell.row, 9, new_obs)        # Obs
                ws.update_cell(cell.row, 10, "Concluido")   # Status
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
    lista_servicos_db = db.get_services_list()

    with st.sidebar:
        st.header("Gestão de Frota")
        st.caption("🔄 Sincronização automática via GitHub")
        if st.button("Atualizar Tela (F5)", use_container_width=True):
            st.rerun()
        st.divider()
        
        with st.expander("🚗 Atualizar KM Manual", expanded=True):
            df_manuais = db.get_dataframe("veiculos_manuais")
            lista_manuais = df_manuais['placa'].tolist() if not df_manuais.empty else []
            placa_manual = st.selectbox("Selecione Manual", lista_manuais)
            if placa_manual:
                km_atual_manual = 0.0
                linha_atual = df_manuais[df_manuais['placa'] == placa_manual]
                if not linha_atual.empty:
                    km_atual_manual = float(linha_atual.iloc[0]['odometro'])
                novo_km_manual = st.number_input("Novo KM", value=km_atual_manual, step=100.0)
                if st.button("Salvar KM"):
                    if db.update_manual_km(placa_manual, novo_km_manual):
                        st.success("Atualizado!"); time.sleep(1); st.rerun()
            
            with st.popover("➕ Cadastrar Novo Manual"):
                novo_placa = st.text_input("Nova Placa")
                novo_km_ini = st.number_input("KM Inicial", value=0.0)
                if st.button("Criar"):
                    if db.update_manual_km(novo_placa, novo_km_ini):
                        st.success("Criado!"); time.sleep(1); st.rerun()

    st.title("🚛 Painel de Controle")

    df_v_sascar = db.get_dataframe("vehicles")
    df_pos_sascar = db.get_dataframe("positions")
    df_v_manual = db.get_dataframe("veiculos_manuais")
    df_logs = db.get_dataframe("maintenance_logs")
    
    mapa_km_total = {}
    if not df_pos_sascar.empty:
        df_pos_sascar['timestamp'] = pd.to_datetime(df_pos_sascar['timestamp'], errors='coerce')
        df_pos_sascar['odometro'] = pd.to_numeric(df_pos_sascar['odometro'], errors='coerce')
        last_pos = df_pos_sascar.sort_values('timestamp').groupby('id_veiculo').tail(1)
        if not df_v_sascar.empty:
            map_id_placa = dict(zip(df_v_sascar['id_veiculo'].astype(str), df_v_sascar['placa']))
            for _, row in last_pos.iterrows():
                p_id = str(row['id_veiculo'])
                p_placa = map_id_placa.get(p_id, row['placa']) 
                mapa_km_total[p_placa] = row['odometro']
    if not df_v_manual.empty:
        for _, row in df_v_manual.iterrows():
            mapa_km_total[row['placa']] = float(row['odometro'])

    todas_placas = list(mapa_km_total.keys())
    todas_placas.sort()

    tab_pend, tab_novo, tab_hist = st.tabs(["🚦 Pendências", "➕ Novo Lançamento", "📚 Histórico"])

    # --- ABA 1: PENDÊNCIAS ---
    with tab_pend:
        if not df_logs.empty and 'status' in df_logs.columns:
            pendentes = df_logs[df_logs['status'] != 'Concluido'].copy()
            if not pendentes.empty:
                pendentes['km_restante'] = pd.to_numeric(pendentes['proxima_km'], errors='coerce') - pendentes['placa'].map(mapa_km_total).fillna(0)
                pendentes = pendentes.sort_values('km_restante')

                for index, row in pendentes.iterrows():
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
                                c_bx1, c_bx2 = st.columns(2)
                                dt_bx = c_bx1.date_input("Data Real", datetime.now() - timedelta(hours=3)) # Data Hoje
                                
                                # NOVO: Campo para colocar o KM Realizado no momento da baixa
                                km_real_bx = c_bx2.number_input("KM Realizado (No Painel)", value=km_atual, step=100.0)

                                c_bx3, c_bx4 = st.columns(2)
                                vl_bx = c_bx3.number_input("Valor R$", value=float(row['valor']) if row['valor'] else 0.0)
                                resp_bx = c_bx4.text_input("Responsável", value=row['responsavel']) 
                                obs_bx = st.text_input("Obs")
                                
                                st.divider()
                                
                                # LOGICA DE INTERVALO AUTOMÁTICO
                                # Tenta descobrir qual era o intervalo original (Meta - Base Original)
                                try:
                                    base_antiga = float(row['km_realizada']) if row['km_realizada'] else 0
                                    meta_antiga = float(row['proxima_km'])
                                    intervalo_sugerido = meta_antiga - base_antiga
                                    # Se der negativo ou zero (erro de cadastro), sugere 10k
                                    if intervalo_sugerido <= 0: intervalo_sugerido = 10000.0
                                except:
                                    intervalo_sugerido = 10000.0

                                reagendar_bx = st.checkbox(f"🔄 Reagendar próxima? (Intervalo Sugerido: {intervalo_sugerido:,.0f})")
                                
                                intervalo_final = 0.0
                                if reagendar_bx:
                                    intervalo_final = st.number_input("Intervalo (KM)", value=intervalo_sugerido, step=1000.0)
                                
                                if st.form_submit_button("Concluir"):
                                    # Atualiza com o KM Realizado digitado
                                    db.update_log_status(row['id'], dt_bx, vl_bx, obs_bx, resp_bx, km_real_bx)
                                    
                                    if reagendar_bx:
                                        # Nova meta = KM Realizado Agora + Intervalo
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

                        with c2.expander("✏️ Editar"):
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
                        if c3.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                            db.delete_log(row['id']); st.rerun()
            else:
                st.info("Nenhuma pendência.")

    # --- ABA 2: NOVO LANÇAMENTO ---
    with tab_novo:
        st.subheader("Registrar Manutenção")
        c_sel1, c_sel2 = st.columns(2)
        p_selected = c_sel1.selectbox("Selecione a Placa", todas_placas) if todas_placas else None
        s_selected = c_sel2.selectbox("Selecione o Serviço", lista_servicos_db)
        
        km_atual_auto = 0.0
        if p_selected:
            km_atual_auto = float(mapa_km_total.get(p_selected, 0.0))
        
        st.divider()
        with st.form("form_novo", clear_on_submit=False):
            c1, c2 = st.columns(2)
            km_base = c1.number_input("KM Atual (Base)", value=km_atual_auto, step=100.0)
            intervalo = c2.number_input("Intervalo para próxima (KM)", value=10000.0, step=1000.0)
            prox_calc = km_base + intervalo
            st.info(f"📅 Próxima manutenção será agendada para: **{prox_calc:,.0f} KM**")
            
            c3, c4, c5 = st.columns(3)
            dt_reg = c3.date_input("Data do Serviço", datetime.now() - timedelta(hours=3))
            val_reg = c4.number_input("Valor (R$)", value=0.0, step=10.0)
            resp_reg = c5.text_input("Responsável")
            obs_reg = st.text_area("Observações")
            
            st.divider()
            cc1, cc2 = st.columns(2)
            is_done = cc1.checkbox("✅ Já realizada (Salvar no Histórico)", value=True)
            do_sched = cc2.checkbox("🔄 Agendar a próxima?", value=True)
            
            if st.form_submit_button("💾 Salvar Lançamento"):
                if not p_selected:
                    st.error("Selecione uma placa primeiro.")
                else:
                    stt = "Concluido" if is_done else "Agendado"
                    km_log = km_base if is_done else ""
                    d1 = {"placa": p_selected, "tipo": s_selected, "km": km_log, "data": dt_reg, 
                          "prox_km": prox_calc, "valor": val_reg, "obs": obs_reg, "resp": resp_reg, "status": stt}
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
