import os
import json
import time
import pandas as pd
import gspread
import requests
import xml.etree.ElementTree as ET
import html
from datetime import datetime, timedelta

# --- LÊ CREDENCIAIS DO GITHUB ---
SASCAR_USER = os.environ["SASCAR_USER"]
SASCAR_PASS = os.environ["SASCAR_PASS"]
GCP_JSON = os.environ["GCP_CREDENTIALS"]

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
            r = requests.post(self.url, data=envelope, headers=self.headers, timeout=60)
            return r.status_code, r.content
        except Exception as e:
            print(f"Erro Conexão: {e}")
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
                                dt_obj = dt_obj - timedelta(hours=3) # Ajuste Fuso Brasil
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

def run_sync():
    print("🚀 Robô Iniciado (Modo Turbo - Esvaziar Fila)...")
    
    try:
        creds_dict = json.loads(GCP_JSON)
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open("frota_db") 
    except Exception as e:
        print(f"❌ Erro Google Sheets: {e}")
        return

    svc = SascarService(SASCAR_USER, SASCAR_PASS)

    # 1. Atualiza Veículos
    veiculos = svc.get_vehicles()
    if veiculos:
        try:
            ws_v = sh.worksheet("vehicles")
            ws_v.clear()
            ws_v.append_row(["id_veiculo", "placa"])
            ws_v.append_rows(veiculos)
            print("✅ Veículos atualizados.")
        except: pass

    # 2. LOOP AGRESSIVO (Até 50 pacotes de 1000 = 50.000 posições)
    # Isso garante que vamos chegar no dado mais atual
    todas_posicoes = []
    max_loops = 50 
    
    for i in range(max_loops):
        print(f"⏳ Baixando pacote {i+1}...")
        
        lote = svc.get_positions(qtd=1000)
        
        if not lote: 
            print("⏹️ Fila da Sascar vazia (Chegamos no final).")
            break
            
        todas_posicoes.extend(lote)
        
        # Se vier menos de 1000, significa que acabou a fila
        if len(lote) < 1000: 
            print("⏹️ Último pacote recebido.")
            break
            
        # Pausa mínima para não ser bloqueado por spam
        time.sleep(0.5)

    if todas_posicoes:
        print(f"💾 Processando {len(todas_posicoes)} registros na memória...")
        
        try:
            ws_v = sh.worksheet("vehicles")
            dv = pd.DataFrame(ws_v.get_all_records())
            map_placa = dict(zip(dv['id_veiculo'].astype(str), dv['placa']))
        except: map_placa = {}

        dados_fmt = []
        for p in todas_posicoes:
            placa = map_placa.get(str(p['id_veiculo']), "Desconhecido")
            dados_fmt.append([p['id_pacote'], p['id_veiculo'], placa, p['timestamp'], p['odometro']])

        try:
            ws_pos = sh.worksheet("positions")
            
            # --- OTIMIZAÇÃO CRÍTICA ---
            # Em vez de salvar 50.000 linhas, processamos tudo aqui no Python
            # e salvamos APENAS a última linha de cada caminhão.
            
            # 1. Lê o que já tinha no Sheets
            try:
                existentes = ws_pos.get_all_records()
                df_old = pd.DataFrame(existentes)
            except:
                df_old = pd.DataFrame()

            # 2. Cria DataFrame com o que baixamos agora
            df_new = pd.DataFrame(dados_fmt, columns=["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])
            
            # 3. Junta tudo
            df_total = pd.concat([df_old, df_new])
            
            if not df_total.empty:
                df_total['timestamp'] = pd.to_datetime(df_total['timestamp'], errors='coerce')
                
                # 4. A Mágica: Mantém apenas o registro mais recente de cada placa
                df_limpo = df_total.sort_values('timestamp').drop_duplicates(subset=['placa'], keep='last')
                
                # 5. Formata a data de volta para string
                df_limpo['timestamp'] = df_limpo['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # 6. Sobrescreve a planilha (agora ela fica leve, com poucas linhas)
                ws_pos.clear()
                ws_pos.append_row(["id_pacote", "id_veiculo", "placa", "timestamp", "odometro"])
                ws_pos.append_rows(df_limpo.values.tolist())
                
                print(f"✅ SUCESSO! Base atualizada. (Baixados: {len(todas_posicoes)} -> Salvos: {len(df_limpo)})")
        except Exception as e:
            print(f"❌ Erro ao salvar: {e}")
    else:
        print("💤 Nenhum dado novo encontrado.")

if __name__ == "__main__":
    run_sync()
