import pandas as pd
import joblib
from blob_connection import read_blob_file,read_joblib_file
def get_dfu_results(ma_list,monthly_sales_perakende, df_gaz,
                 df_dolar, df_ruhsat, df_konut, df_konut_faiz, df_tufe,
                 df_fiyat, df_fiyat_dolar, kanal, duration):
    dfu_list = monthly_sales_perakende.columns.tolist()
    pred_list = []

    for i in range(len(dfu_list)):
        #file_1m = pd.read_csv('disveri_included/perakende/3m_file.csv')
        #file_1m = pd.read_csv('disveri_included/' + kanal + '/1m_file.csv')
        #file_3m = pd.read_csv('disveri_included/' + kanal + '/3m_file.csv')
        
        file_m = read_blob_file('disveri_included/' + kanal + '/' +
                             duration +'_file.csv')

        
        dfu = dfu_list[i]

        #dfu = 'SKM DUV 15X15+17,5'
        best_file = file_m[file_m['Unnamed: 0'] == dfu].iloc[0,1]
        
        best_file = best_file.split('_')[-1]
        best_file = best_file.split('.')[0]
        isma_in = int(best_file[0])
        isfiyatdolar_in = int(best_file[1])
        isfiyat_in = int(best_file[2])
        isgaz_in = int(best_file[3])
        istufe_in = int(best_file[4])
        isdolar_in = int(best_file[5])
        iskonut_in = int(best_file[6])
        iskonutfaiz_in = int(best_file[7])
        isruhsat_in = int(best_file[8])
        
        ma_list = [1,3,6]
        
        X_all_pred = pd.DataFrame()
        X_fiyat_pred = pd.DataFrame()
        X_fiyat_dolar_pred = pd.DataFrame()
        
        for ma in ma_list:
            X_all_pred['input'+str(ma)] = monthly_sales_perakende[dfu].rolling(window=ma).mean().shift(0).values
            if dfu in df_fiyat.columns.tolist():
                X_fiyat_pred['fiyat'+str(ma)] = df_fiyat[dfu].rolling(window=ma).mean().shift(0).values
        
            if dfu in df_fiyat_dolar.columns.tolist():
                X_fiyat_dolar_pred['fiyat_dolar'+str(ma)] = df_fiyat_dolar[dfu].rolling(window=ma).mean().shift(0).values
        
        
        
        X_all_pred.index = monthly_sales_perakende.index
        X_fiyat_pred.index = df_fiyat.index
        X_fiyat_dolar_pred.index = df_fiyat_dolar.index
        
        if isfiyat_in == 1:
            X_all_pred = X_all_pred.merge(X_fiyat_pred, how='left', left_index = True, right_index = True)
        
        if isfiyatdolar_in == 1:
            X_all_pred = X_all_pred.merge(X_fiyat_dolar_pred, how='left', left_index = True, right_index = True)
        
        if (isma_in == 0):
            X_all_pred = X_all_pred.iloc[:, len(ma_list):]
        
        
            
        X_gaz_pred = pd.DataFrame()
        X_dolar_pred = pd.DataFrame()
        X_ruhsat_pred = pd.DataFrame()
        X_konut_pred = pd.DataFrame()
        X_konutfaiz_pred = pd.DataFrame()
        X_tufe_pred = pd.DataFrame()
        for ma in ma_list:
            X_gaz_pred['gaz'+str(ma)] = df_gaz['TL/kwh'].rolling(window=ma).mean().shift(0).values
            X_dolar_pred['dolar'+str(ma)] = df_dolar['TP DK USD A YTL'].rolling(window=ma).mean().shift(0).values
            X_ruhsat_pred['ruhsat'+str(ma)] = df_ruhsat['Toplam_alan'].rolling(window=ma).mean().shift(0).values
            X_konut_pred['konut'+str(ma)] = df_konut['Konut Satış'].rolling(window=ma).mean().shift(0).values
            X_konutfaiz_pred['faiz'+str(ma)] = df_konut_faiz['Konut Kredisi Faiz Oranları'].rolling(window=ma).mean().shift(0).values
            X_tufe_pred['tufe'+str(ma)] = df_tufe['value'].rolling(window=ma).mean().shift(0).values
        
        
        X_gaz_pred.index = df_gaz.index
        X_dolar_pred.index = df_dolar.index
        X_ruhsat_pred.index = df_ruhsat.index
        X_konut_pred.index = df_konut.index
        X_konutfaiz_pred.index = df_konut_faiz.index
        X_tufe_pred.index = df_tufe.index
        
        if isgaz_in == 1:
            X_all_pred = X_all_pred.merge(X_gaz_pred, how='left', left_index = True, right_index = True)
        
        if isdolar_in == 1:
            X_all_pred  = X_all_pred.merge(X_dolar_pred , how='left', left_index = True, right_index = True)
        
        if iskonut_in == 1:
            X_all_pred  = X_all_pred.merge(X_konut_pred, how='left', left_index = True, right_index = True)
        
        if iskonutfaiz_in == 1:
            X_all_pred = X_all_pred.merge(X_konutfaiz_pred, how='left', left_index = True, right_index = True)
        
        if isruhsat_in == 1:
            X_all_pred = X_all_pred.merge(X_ruhsat_pred, how='left', left_index = True, right_index = True)
        
        if istufe_in == 1:
            X_all_pred = X_all_pred.merge(X_tufe_pred, how='left', left_index = True, right_index = True)
        
        
        
        print("tahmin üretiliyor")
        X_all_pred = X_all_pred.bfill()
        X_all_pred = X_all_pred.ffill()
        # X_all_pred = X_all_pred.fillna(method = 'bfill')
        # X_all_pred = X_all_pred.fillna(method = 'ffill')
        X_all_pred = X_all_pred.fillna(0)
        
        #regr_1m= joblib.load('models_3m_perakende/'+dfu+".joblib")
        #regr_3m = joblib.load('models_3m_perakende/'+dfu+".joblib")
        
        regr = read_joblib_file('models_'+ duration +'_' + kanal +'/'+dfu+".joblib")


        
        X_test = X_all_pred.values[len(X_all_pred)-1:len(X_all_pred),:]
        # Bu bize bir olasılık verecek. Eğer olasılık threshold'un üstündeyse hata olacak diyoruz
        # pred_1m = regr_1m.predict(X_test)[0]
        # pred_3m = regr_3m.predict(X_test)[0]
        
        pred = regr.predict(X_test)[0]



        # pred_list_1m.append(pred_1m)
        # pred_list_3m.append(pred_3m)
        pred_list.append(pred)
        
    return pred_list
        
