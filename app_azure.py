import pandas as pd
import panel as pn
import datetime
from veri_indir import get_monthly_sales
import joblib
from get_sku_features import get_features
from get_dfu_features import get_dfu_results
import io
from tqdm import tqdm
import zipfile
from blob_connection import read_blob_file,write_blob_file,read_joblib_file

# Initialize the Panel extension
pn.extension()
# Kullanıcı adı ve şifre doğrulaması için sabitler
CORRECT_USERNAME = "kalevam"
CORRECT_PASSWORD = "kalevam2024"

# Giriş formu elemanları
username_input = pn.widgets.TextInput(name="Kullanıcı Adı", placeholder="Kullanıcı adınızı girin")
password_input = pn.widgets.PasswordInput(name="Şifre", placeholder="Şifrenizi girin")
login_button = pn.widgets.Button(name="Giriş Yap", button_type="primary")
login_alert = pn.pane.Alert("", alert_type="danger", visible=False)

# Ana içerik alanı - Dinamik içerik burada güncellenecek
main_area = pn.Column()

# Giriş kontrol fonksiyonu
def check_login(event):
    username = username_input.value
    password = password_input.value
    if username == CORRECT_USERNAME and password == CORRECT_PASSWORD:
        # Giriş başarılı
        login_alert.object = "Giriş başarılı!"
        login_alert.alert_type = "success"
        login_alert.visible = True
        # Mevcut içeriği temizle ve ana içeriği tabs_machine ile güncelle
        main_area.objects = [tabs_machine]  # main_area'nin içeriğini tabs_machine ile değiştir
    else:
        # Giriş başarısız
        login_alert.object = "Kullanıcı adı veya şifre hatalı!"
        login_alert.alert_type = "danger"
        login_alert.visible = True

login_button.on_click(check_login)
login_form = pn.Column(username_input, password_input, login_button, login_alert)
main_area.append(login_form)

# Function to be executed on the first day of each month
def monthly_task(kanal):
    print(f"Monthly task executed at {datetime.datetime.now()}")
    
    df_mapping_p = read_blob_file('dfu_malzeme_mapping/'+kanal + '.csv', index_col = 0)

    print('dfu_malzeme_mapping OKUNUYOR!!!!!!!!!!!!')
    df_mapping_p['Malzeme'] = df_mapping_p['Malzeme'].apply(lambda x: str(x))
    today = datetime.date.today()
    if today.month == 1:
        month_year = '12' + str(today.year-1)
    elif today.month < 11:
        month_year = '0'+str(today.month-1) + str(today.year)
    else:
        month_year = str(today.month-1) + str(today.year)
    if kanal == 'perakende':
        df_perakende = get_monthly_sales(month_year,'11')
    elif kanal == 'kurumsal':
        df_perakende = get_monthly_sales(month_year,'12')
    elif kanal == 'yurtdisi':
        df_perakende = get_monthly_sales(month_year,'13')
        
    df_perakende['ZFIILI_MIKTAR_TOB'] = df_perakende['ZFIILI_MIKTAR_TOB'].apply(lambda x:
                                                                                float(x))
    df_perakende['ZFIILI_NET_TL'] = df_perakende['ZFIILI_NET_TL'].apply(lambda x:
                                                                                float(x))
    
    dict_mapping = df_mapping_p.set_index('Malzeme')['DFU']
    
    df_perakende['DFU'] = df_perakende['A0MATERIAL'].map(dict_mapping)
    df_perakende['zaman'] = pd.to_datetime(df_perakende['A0CALMONTH'], format = '%Y%m')
    df_perakende.set_index('zaman', inplace = True)
    
    sales_month = df_perakende.groupby([pd.Grouper(freq = '1M'),'DFU'])['ZFIILI_MIKTAR_TOB'].sum()
    sales_month = sales_month.reset_index()
    sales_month = sales_month.pivot(index = 'zaman', columns = 'DFU',
                                    values = 'ZFIILI_MIKTAR_TOB')
    
    monthly_sales_perakende = read_blob_file('monthly_sales/' + kanal + '.csv')

    monthly_sales_perakende['zaman'] = pd.to_datetime(monthly_sales_perakende['zaman'])
    monthly_sales_perakende.set_index('zaman', inplace = True)
    
    monthly_sales_perakende = pd.concat([monthly_sales_perakende, sales_month], axis = 0)
    
    df_perakende_p = df_perakende[df_perakende['ZFIILI_MIKTAR_TOB'] > 0]
    
    sales_month_p = df_perakende_p.groupby([pd.Grouper(freq = '1M'),'DFU'])['ZFIILI_MIKTAR_TOB'].sum()
    sales_month_p = sales_month_p.reset_index()
    sales_month_p = sales_month_p.pivot(index = 'zaman', columns = 'DFU',
                                    values = 'ZFIILI_MIKTAR_TOB')
    revenue_month_p = df_perakende_p.groupby([pd.Grouper(freq = '1M'),'DFU'])['ZFIILI_NET_TL'].sum()
    revenue_month_p = revenue_month_p.reset_index()
    revenue_month_p = revenue_month_p.pivot(index = 'zaman', columns = 'DFU',
                                    values = 'ZFIILI_NET_TL')
    
    fiyat_month = revenue_month_p/sales_month_p
    
    fiyat_month_dolar = fiyat_month/float(dolar_input.value)
    if kanal == 'perakende' or kanal == 'kurumsal':
        df_fiyat = read_blob_file('Dış Veri - Formatted/fiyat_dfu_perakende.csv')
        df_fiyat['zaman'] = pd.to_datetime(df_fiyat['zaman'])
        df_fiyat.set_index('zaman', inplace = True)
        df_fiyat = pd.concat([df_fiyat, fiyat_month], axis = 0)
    
        df_fiyat_dolar = read_blob_file('Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv')
        df_fiyat_dolar['zaman'] = pd.to_datetime(df_fiyat_dolar['zaman'])
        df_fiyat_dolar.set_index('zaman', inplace = True)
        df_fiyat_dolar = pd.concat([df_fiyat_dolar, fiyat_month_dolar], axis = 0)
        df_fiyat_dolar = df_fiyat_dolar[~df_fiyat_dolar.index.duplicated(keep='last')]
        df_fiyat_dolar.reset_index(inplace=True)
       # df_fiyat_dolar.to_csv('Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv')
        print('Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
        write_blob_file(df=df_fiyat_dolar, file_name='Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv')
        
        df_fiyat = df_fiyat[~df_fiyat.index.duplicated(keep='last')]
        df_fiyat.reset_index(inplace=True)
       # df_fiyat.to_csv('Dış Veri - Formatted/fiyat_dfu_perakende.csv')
        print('Dış Veri - Formatted/fiyat_dfu_perakende.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
        write_blob_file(df=df_fiyat, file_name='Dış Veri - Formatted/fiyat_dfu_perakende.csv')
    else:
        df_fiyat = read_blob_file('Dış Veri - Formatted/fiyat_dfu_yurtdisi.csv')
        df_fiyat['zaman'] = pd.to_datetime(df_fiyat['zaman'])
        df_fiyat.set_index('zaman', inplace = True)
        df_fiyat = pd.concat([df_fiyat, fiyat_month], axis = 0)
    
        df_fiyat_dolar = read_blob_file('Dış Veri - Formatted/fiyat_dolar_dfu_yurtdisi.csv')
        df_fiyat_dolar['zaman'] = pd.to_datetime(df_fiyat_dolar['zaman'])
        df_fiyat_dolar.set_index('zaman', inplace = True)
        df_fiyat_dolar = pd.concat([df_fiyat_dolar, fiyat_month_dolar], axis = 0)
        df_fiyat_dolar = df_fiyat_dolar[~df_fiyat_dolar.index.duplicated(keep='last')]
        df_fiyat_dolar.reset_index(inplace=True)    
        print('Dış Veri - Formatted/fiyat_dolar_dfu_yurtdisi.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
        #df_fiyat_dolar.to_csv('Dış Veri - Formatted/fiyat_dolar_dfu_yurtdisi.csv')
        write_blob_file(df=df_fiyat_dolar, file_name='Dış Veri - Formatted/fiyat_dolar_dfu_yurtdisi.csv')
        
        df_fiyat = df_fiyat[~df_fiyat.index.duplicated(keep='last')]
        df_fiyat.reset_index(inplace=True)
        #df_fiyat.to_csv('Dış Veri - Formatted/fiyat_dfu_yurtdisi.csv')
        print('Dış Veri - Formatted/fiyat_dfu_yurtdisi.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
        write_blob_file(df=df_fiyat, file_name='Dış Veri - Formatted/fiyat_dfu_yurtdisi.csv')
        
    monthly_sales_perakende = monthly_sales_perakende[~monthly_sales_perakende.index.duplicated(keep='last')]
    monthly_sales_perakende.reset_index(inplace=True)
    #monthly_sales_perakende.to_csv('monthly_sales/' + kanal + '.csv')
    print('monthlysalesYAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
    write_blob_file(df=monthly_sales_perakende, file_name='monthly_sales/' + kanal + '.csv')
    
    dfu_list = monthly_sales_perakende.columns.tolist()
    for i in tqdm(range(1,len(dfu_list))):
        dfu = dfu_list[i]
        print("kanal:",kanal)
        print("dfu:",dfu)
        #print('sales_sku/'+kanal+'/'+dfu+')
        #monthly_sales_sku = read_blob_file('sales_sku/perakende/'+dfu+'.csv')
        monthly_sales_sku = read_blob_file('sales_sku/'+kanal+'/'+dfu+'.csv')
        print ('sales_sku/'+kanal+'/'+dfu+'.csv')

        monthly_sales_sku['zaman'] = pd.to_datetime(monthly_sales_sku['zaman'])
        monthly_sales_sku.set_index('zaman', inplace = True)
        
        df_perakende_p_dfu = df_perakende_p[df_perakende_p['DFU'] == dfu]
        
        sales_sku = df_perakende_p_dfu.groupby([pd.Grouper(freq = '1M'), 'A0MATERIAL'])[
            'ZFIILI_MIKTAR_TOB'].sum()
        
        sales_sku = sales_sku.reset_index()
        sales_sku = sales_sku.pivot(index = 'zaman', columns = 'A0MATERIAL',
                                    values = 'ZFIILI_MIKTAR_TOB')
           
        monthly_sales_sku = pd.concat([monthly_sales_sku, sales_sku], axis = 0)
        
        if 'Fiili Miktar (TÖB)' in monthly_sales_sku.columns.tolist():
            monthly_sales_sku = monthly_sales_sku.drop('Fiili Miktar (TÖB)', axis=1)
        
        monthly_sales_sku = monthly_sales_sku.fillna(0)
        monthly_sales_sku = monthly_sales_sku[~monthly_sales_sku.index.duplicated(keep='last')]
        monthly_sales_sku.reset_index(inplace=True)
        #monthly_sales_sku.to_csv('sales_sku/' +kanal + '/'+dfu+'.csv')
        print('sales_sku/ YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
        write_blob_file(df=monthly_sales_sku, file_name='sales_sku/' +kanal + '/'+dfu+'.csv')


# Function to check if today is the 1st of the month
def check_and_run_task():
    today = datetime.date.today()
    if today.day == 1:  # Check if it's the 1st of the month
        monthly_task('perakende')
        monthly_task('kurumsal')
        monthly_task('yurtdisi')


# Add a periodic callback that checks every day (86400000 ms = 24 hours)
pn.state.add_periodic_callback(check_and_run_task, period=86400000)  # Checks once a day



gaz_input = pn.widgets.TextInput(name='Doğalgaz Birim Fiyatı', placeholder='Buraya fiyat gir') 
dolar_input = pn.widgets.TextInput(name='DolarFiyatı', placeholder='Buraya fiyat gir')
konut_satis_input = pn.widgets.TextInput(name='2.El Konut Satışı', placeholder='Buraya adet gir')
konut_faiz_input = pn.widgets.TextInput(name='Konut Faizi-Yıllık', placeholder='Buraya faiz gir')
ruhsat_input = pn.widgets.TextInput(name='İnşaat Ruhsatı (m2)', placeholder='Buraya alan gir')
tufe_input = pn.widgets.TextInput(name='TÜFE-Yıllık', placeholder='Buraya enflasyon gir')


#message_outer = pn.widgets.StaticText(value='İşlem henüz başlamadı')
#message_inner = pn.widgets.StaticText(value='İşlem henüz başlamadı')

def tahminleri_olustur():
    print('Tahminler Oluşturuluyor!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    zip_buffer = io.BytesIO()
    kanal_list = ['perakende', 'yurtdisi', 'kurumsal']
    
    # Her tahmin oluşturuldu aylık satış verileri kontrol edilip bir önceki ayki veri
    #yoksa ekleme yapılıyor.
    monthly_sales_to_check = read_blob_file('monthly_sales/yurtdisi.csv')
    monthly_sales_to_check['zaman'] = pd.to_datetime(monthly_sales_to_check['zaman'])
    last_month = monthly_sales_to_check['zaman'].dt.month.iloc[-1]
    today = datetime.date.today()
    if today.month == 1:
        if last_month != 12:
            monthly_task('perakende')
            monthly_task('kurumsal')
            monthly_task('yurtdisi')
    else:
        if last_month != today.month -1:
            monthly_task('perakende')
            monthly_task('kurumsal')
            monthly_task('yurtdisi')
            
    
    # Create a ZIP file in the buffer
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        print('TEXTBOXLAR Kaydediliyor!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        for kanal in kanal_list:
            #kanal = 'yurtdisi'
            monthly_sales_perakende = read_blob_file('monthly_sales/' + kanal + '.csv')
            total_iterations = len(kanal_list)
            #message_outer.value = f" Dağıtım Kanalı İterasyonları {k + 1}/{total_iterations}"
            
            monthly_sales_perakende['zaman'] = pd.to_datetime(monthly_sales_perakende['zaman'])
            monthly_sales_perakende.set_index('zaman', inplace = True)
            
            a = monthly_sales_perakende.sum(axis = 0)
            
            a = a[a==0]
            columns_drop = a.index.tolist()
            monthly_sales_perakende = monthly_sales_perakende.drop(columns_drop, axis=1)
            
            
            print('Dış Veri - Formatted/fiyat_dfu_perakende.csv OKUNUYOOORRRRRRRRR!!!!!!!!!!!!')
            df_fiyat = read_blob_file('Dış Veri - Formatted/fiyat_dfu_perakende.csv')
            df_fiyat['zaman'] = pd.to_datetime(df_fiyat['zaman'])
            df_fiyat.set_index('zaman', inplace = True)
            
            print('Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv OKUNUYOOORRRRRRRRR!!!!!!!!!!!!')
            
            df_fiyat_dolar = read_blob_file('Dış Veri - Formatted/fiyat_dolar_dfu_perakende.csv')
            df_fiyat_dolar['zaman'] = pd.to_datetime(df_fiyat_dolar['zaman'])
            df_fiyat_dolar.set_index('zaman', inplace = True)
            
            df_gaz = read_blob_file('Dış Veri - Formatted/Dogalgaz_fiyatları.csv')
            df_gaz['zaman'] = pd.to_datetime(df_gaz['zaman'])
            df_gaz.set_index('zaman', inplace = True)
            df_gaz.loc[len(df_gaz)] = float(gaz_input.value)
            df_gaz.rename(index={df_gaz.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_gaz = df_gaz[~df_gaz.index.duplicated(keep='last')]
            df_gaz.reset_index(inplace=True)
            #df_gaz.to_csv('Dış Veri - Formatted/Dogalgaz_fiyatları.csv')
            print('Dış Veri - Formatted/Dogalgaz_fiyatları.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_gaz, file_name='Dış Veri - Formatted/Dogalgaz_fiyatları.csv')
            
            df_dolar = read_blob_file('Dış Veri - Formatted/dolar_tl.csv')
            df_dolar['zaman'] = pd.to_datetime(df_dolar['zaman'])
            df_dolar.set_index('zaman', inplace = True)
            df_dolar.loc[len(df_dolar)] = float(dolar_input.value)
            df_dolar.rename(index={df_dolar.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_dolar = df_dolar[~df_dolar.index.duplicated(keep='last')]
            df_dolar.reset_index(inplace=True)
            #df_dolar.to_csv('Dış Veri - Formatted/dolar_tl.csv')
            print('Dış Veri - Formatted/dolar_tl.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_dolar, file_name='Dış Veri - Formatted/dolar_tl.csv')
            
            
            df_konut = read_blob_file('Dış Veri - Formatted/konut_2el_satis.csv')
            df_konut['zaman'] = pd.to_datetime(df_konut['zaman'])
            df_konut.set_index('zaman', inplace = True)
            df_konut.loc[len(df_konut)] = float(konut_satis_input.value)
            df_konut.rename(index={df_konut.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_konut = df_konut[~df_konut.index.duplicated(keep='last')]
            df_konut.reset_index(inplace=True)
            #df_konut.to_csv('Dış Veri - Formatted/konut_2el_satis.csv')
            print('Dış Veri - Formatted/konut_2el_satis.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_konut, file_name='Dış Veri - Formatted/konut_2el_satis.csv')
            
            
            df_konut_faiz = read_blob_file('Dış Veri - Formatted/konut_faiz.csv')
            df_konut_faiz['zaman'] = pd.to_datetime(df_konut_faiz['zaman'])
            df_konut_faiz.set_index('zaman', inplace = True)
            df_konut_faiz.loc[len(df_konut_faiz)] = float(konut_faiz_input.value)
            df_konut_faiz.rename(index={df_konut_faiz.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_konut_faiz = df_konut_faiz[~df_konut_faiz.index.duplicated(keep='last')]
            df_konut_faiz.reset_index(inplace=True)
            #df_konut_faiz.to_csv('Dış Veri - Formatted/konut_faiz.csv')
            print('Dış Veri - Formatted/konut_faiz.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_konut_faiz, file_name='Dış Veri - Formatted/konut_faiz.csv')
            
            
            df_ruhsat = read_blob_file('Dış Veri - Formatted/ruhsat_izinler_yeni.csv')
            df_ruhsat['zaman'] = pd.to_datetime(df_ruhsat['zaman'])
            df_ruhsat.set_index('zaman', inplace = True)
            df_ruhsat.loc[len(df_ruhsat)] = float(ruhsat_input.value)
            df_ruhsat.rename(index={df_ruhsat.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_ruhsat = df_ruhsat[~df_ruhsat.index.duplicated(keep='last')]
            df_ruhsat.reset_index(inplace=True)
            #df_ruhsat.to_csv('Dış Veri - Formatted/ruhsat_izinler_yeni.csv')
            print('Dış Veri - Formatted/ruhsat_izinler_yeni.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_ruhsat, file_name='Dış Veri - Formatted/ruhsat_izinler_yeni.csv')
            
            
            df_tufe = read_blob_file('Dış Veri - Formatted/TUFE_yillik.csv')
            df_tufe['zaman'] = pd.to_datetime(df_tufe['zaman'])
            df_tufe.set_index('zaman', inplace = True)
            df_tufe.loc[len(df_tufe)] = float(tufe_input.value)
            df_tufe.rename(index={df_tufe.index[-1]:df_fiyat_dolar.index[-1]},inplace=True)
            df_tufe = df_tufe[~df_tufe.index.duplicated(keep='last')]
            df_tufe.reset_index(inplace=True)
            #df_tufe.to_csv('Dış Veri - Formatted/TUFE_yillik.csv')
            print('Dış Veri - Formatted/TUFE_yillik.csv YAZILIYORRRRRRRRRRRRRRRRR!!!!!!!!!!!!')
            write_blob_file(df=df_tufe, file_name='Dış Veri - Formatted/TUFE_yillik.csv')
            
            ###########################################################################################
            
            dfu_list = monthly_sales_perakende.columns.tolist()
            print('TEXTBOXLAR KAYDEDİLDİİİİİ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')   
            pred_list_1m_perakende = get_dfu_results([1,3,6],monthly_sales_perakende, df_gaz,
                             df_dolar, df_ruhsat, df_konut, df_konut_faiz, df_tufe,
                             df_fiyat, df_fiyat_dolar, kanal, '1m')
            
            pred_list_3m_perakende = get_dfu_results([1,3,6],monthly_sales_perakende, df_gaz,
                             df_dolar, df_ruhsat, df_konut, df_konut_faiz, df_tufe,
                             df_fiyat, df_fiyat_dolar, kanal, '3m')
            
            df_result_all = pd.DataFrame()
            df_result_all['DFU'] = dfu_list
            df_result_all['Tahmini Satış - Sonraki Ay'] = pred_list_1m_perakende
            df_result_all['Tahmini Satış - Sonraki 3 Ay'] = pred_list_3m_perakende
            
            df_result_all = df_result_all.sort_values(by = 'DFU')
            
            output = io.BytesIO()
            ############################################################################################
            
                #dfu = 'SKM YER 45X45'
                
            with pd.ExcelWriter(output) as writer:
               
                # use to_excel function and specify the sheet_name and index 
                # to store the dataframe in specified sheet
                df_result_all.to_excel(writer, sheet_name='DFU Tahminleri', index=False)
            
                        
                        
                for i in tqdm(range(len(dfu_list))):
                    dfu = dfu_list[i]
                    #monthly_sales_sku = read_blob_file('sales_sku/kurumsal/'+dfu+'.csv')
                    #dfu = 'SKM DUV 20X20+25'
                    print("dfu",dfu)
                    monthly_sales_sku = read_blob_file('sales_sku/' +kanal +'/'+dfu+'.csv')
                    total_iterations_dfu = len(dfu_list)
                    #message_inner.value = f"DFU-SKU İterasyonları {i + 1}/{total_iterations_dfu}"

    
                    monthly_sales_sku['zaman'] = pd.to_datetime(monthly_sales_sku['zaman'])
                    monthly_sales_sku.set_index('zaman', inplace = True)
                    
                    if 'Fiili Miktar (TÖB)' in monthly_sales_sku.columns.tolist():
                        monthly_sales_sku = monthly_sales_sku.drop('Fiili Miktar (TÖB)', axis=1)
                    
                    a = monthly_sales_sku.iloc[-12:,:]
                    a = a.sum(axis = 0)
                    a = a[a>0]
                    
                    
                    
                    df_mapping = read_blob_file('models_sku/' + kanal + '/'+dfu+'_mapping.csv')
                    
                    test_sku = a.index.tolist()
                    sku_pd_list = []
                    pred_list_1m = []
                    
                    pred_list_3m = []
                    
                    ma_list = [1,3,6]
                    regr = read_joblib_file('models_sku/' + kanal + '/'+dfu+"_1m.joblib")
                    regr_3m = read_joblib_file('models_sku/' + kanal + '/'+dfu+"_3m.joblib")
                    for sku in test_sku:
                        X_test = get_features(ma_list, 0, sku,
                                              monthly_sales_sku.iloc[-6:,:],
                                              df_gaz.iloc[-6:,:], df_dolar.iloc[-6:,:], df_ruhsat.iloc[-6:,:], 
                                              df_konut.iloc[-6:,:], df_konut_faiz.iloc[-6:,:], 
                                              df_tufe.iloc[-6:,:], df_mapping)
                        X_test = X_test.fillna(method = 'bfill')
                        X_test = X_test.fillna(method = 'ffill')
                        X_test = X_test.fillna(0)
                        
                        sku_pd_list.append(sku)
                    
                        pred_list_1m.append(regr.predict(X_test.values[-1,:].reshape(1,-1))[0])
                        pred_list_3m.append(regr_3m.predict(X_test.values[-1,:].reshape(1,-1))[0])
                    
                    df_result_sku = pd.DataFrame()
                    df_result_sku['SKU'] = test_sku
                    df_result_sku['Tahmini Satış - Sonraki Ay'] = pred_list_1m
                    df_result_sku['Tahmini Satış - Sonraki 3 Ay'] = pred_list_3m
                    dfu_tahmin = df_result_all[df_result_all['DFU'] == dfu]
                    sku_tahmin_aylik = df_result_sku['Tahmini Satış - Sonraki Ay'].sum()
                    sku_tahmin_3aylik = df_result_sku['Tahmini Satış - Sonraki 3 Ay'].sum()
                    dfu_tahmin_aylik = dfu_tahmin['Tahmini Satış - Sonraki Ay'].iloc[0]
                    dfu_tahmin_3aylik = dfu_tahmin['Tahmini Satış - Sonraki 3 Ay'].iloc[0]
                    if len(df_result_sku) > 0:
                        if sku_tahmin_aylik != 0:
                            df_result_sku['Tahmini Satış - Sonraki Ay'] = df_result_sku['Tahmini Satış - Sonraki Ay'] .apply(
                                lambda x: x*dfu_tahmin_aylik/sku_tahmin_aylik)
                        if sku_tahmin_3aylik != 0:
                            df_result_sku['Tahmini Satış - Sonraki 3 Ay'] = df_result_sku['Tahmini Satış - Sonraki 3 Ay'] .apply(
                            lambda x: x*dfu_tahmin_3aylik/sku_tahmin_3aylik)
                        df_result_sku = df_result_sku.sort_values(by = 'SKU')
                    
                    df_result_sku.to_excel(writer, sheet_name=dfu, index=False)
            output.seek(0)
            zf.writestr(kanal+'_tahmin.xlsx', output.read())
    zip_buffer.seek(0)
    return zip_buffer




# Create a FileDownload widget with the file generation function
download_button = pn.widgets.FileDownload(
    filename='tahminler.zip',
    callback=tahminleri_olustur,  # Callback to generate the file
    button_type="primary",       # Styling of the button (optional)
    label="Download Excel"       # Button label
)
component_general_analysis = pn.Column(pn.Row(gaz_input,dolar_input, konut_satis_input),
                                       pn.Row(konut_faiz_input,ruhsat_input, tufe_input)
                                   , pn.Column("## Excel Olarak Tahminleri İndir", download_button))
                                   #button_tahmin,pn.panel(pn.bind(tahminleri_olustur, button_tahmin), loading_indicator=True))

tabs_machine = pn.Tabs(
    ('Satış Tahminleri', component_general_analysis),
    dynamic=False
    )

#  Detect-Predict-Optimize
text = """
### Bu uygulama Kale Seramik Satışlarının DFU ve SKU bazında tahminlerini yapar. Tahminler bu ayda ve sonraki 3 ayda olacak satışları kapsar.
### İndirilen zip dosyası içerisinde şu tahminler yer alır:
### 1) Perakende dağıtım kanalı için tahminler
### 2) Yurtdışı dağıtım kanalı için tahminler
### 3) Kurumsal dağıtım kanalı için tahminler
    """

#file_input = pn.widgets.FileInput(align='center', sizing_mode='stretch_width')

# button_des = pn.widgets.Button(name='Run Descriptive Analysis', sizing_mode='stretch_width')
# button_anomaly = pn.widgets.Button(name='Start Anomaly Analysis', sizing_mode='stretch_width')

widgets = pn.WidgetBox(
    pn.panel( text,margin=(0, 16)),
    margin=0,
    sizing_mode='stretch_width'
    #width = 200
)

template = pn.template.MaterialTemplate(title="Kale Seramik Satış Tahmin Uygulaması")

template.sidebar.append(widgets)
# explanation = """
# """

#template.sidebar.append(pn.panel(explanation, sizing_mode='stretch_width'))
#template.main.append( tabs_machine)
template.main.append(main_area)
template.servable()

# create a excel writer object
