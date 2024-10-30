import requests
import pandas as pd

# İlk satır Servisin endpointi
# ikinci satır sorgu adı ve filtreler ( sql'de where koşulu gibi)
# üçüncü satırda select'ten sonra gelmesini istediğimiz alanları yazıyoruz
# Filtrelerde ilk alan ZVAR_ZCOMPCODE_001 şirket kodu
# ZVAR_SALESORG_001 satış organizasyonu
# ZVAR_0DISTR_CHAN_001 Dağıtım Kanalı
#ZVAR_CALMONTH_002='06.2024',ZVAR_CALMONTH_002To='06.2024' Tarih aralığı
def get_monthly_sales(date, dagitim_kanali):
    url = (
    "https://bwp.kale.com.tr/sap/opu/odata/sap/ZREP_ZKVCCO02_Q004_PO_SRV/"
    f"ZREP_ZKVCCO02_Q004_PO(ZVAR_CALMONTH_002='{date}',ZVAR_CALMONTH_002To='{date}',ZVAR_ZCOMPCODE_001='1000',ZVAR_SALESORG_001='',ZVAR_0DISTR_CHAN_001='{dagitim_kanali}')/"
        
    "Results?$select=A0CALMONTH,"    #AY
    #"A0CALYEAR,"                    #takvim yıl AY
    # "A0COMP_CODE,"                   # Şirket Kodu
    # "A0SALESORG,"                     # Satış organizasyonu
    # "A0SALES_GRP,"                   # Satış Grubu
    # "A0DISTR_CHAN,"                 # Dağıtım kanalı   
    # "A0SALES_OFF,"                  # Satıs bürosu
    # "A0SALES_DIST,"                 # Müşteri Bölgesi
    # "A0SHIP_TO__0COUNTRY,"          # Teslimatin ülkesi
    # "A0SHIP_TO__0REGION,"           # Teslimatın Bölgesi
    #"A0CUSTOMER,"                    # Müşteri
     "A0MATERIAL,"                    # Malzeme
    # "A0MATERIAL__0PROD_HIER,"        # Ürün Hiyerarşisi
    # "A0MATERIAL__ZPRHANAGP,"         #Malzeme_Ana Grup
    # "A0MATERIAL__ZPRHGRUP,"         # Malzeme_Grup
    # "A0MATERIAL__ZPRHALTGP,"        # Malzeme_Alt Grup
    # "A0MATERIAL__ZEBAT,"            # Malzeme Ebat
    # "A0MATERIAL__ZFYTSGMNT,"        # Malzeme_Fiyat Segment
    # "A0MATERIAL__ZSTIL,"            # Malzeme_Stil
    # "A0MATERIAL__ZRT,"              # Malzeme_Renk Tonu
    # "A0MATERIAL__ZTIPOLOJI,"        # Malzeme_Tipoloji
    # "A0MATERIAL__ZSERIADI,"         # Malzeme_Seri Adı
    # "ZPRCELST,"                     # Fiyat Listesi ID
     "ZFIILI_MIKTAR_TOB,"             # Fiili miktar 
    "ZFIILI_NET_TL"                # Fiili Net TL
    # "ZFIILI_VUK_BRUT,"              # Fiili VUK Brüt
    # "ZDGR_TFRS_GELIR")              # DGR-TFRS Gelir 
    )
    # Kimlik doğrulama bilgileri
    auth = ('POSERVICE', 'KalePO1234')
    
    # XML istemek için HTTP başlıklarını ayarlayın
    headers = {'Accept': 'application/json'}
    
    # HTTP GET isteği, verify=False SSL/TLS doğrulamasını devre dışı bırakır (Güvenlik riski oluşturabilir)
    response = requests.get(url, auth=auth, headers=headers, verify=False)
    
    print(response.status_code)
    # Yanıtın kontrolü
    if response.status_code == 200:
        # JSON formatında veriyi alma
        data = response.json()
        print("Veri başarıyla çekildi:")
        print(data)
    else:
        print(f"Veri çekme işlemi başarısız oldu. Hata Kodu: {response.status_code}")
    df = pd.DataFrame(data['d']['results'])
    df = df.drop(columns=["__metadata"])
    return df

