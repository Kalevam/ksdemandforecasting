import pandas as pd
from blob_connection import read_blob_file,read_joblib_file
def get_features(ma_list, shift_value, sku, monthly_sales, df_gaz,
                 df_dolar, df_ruhsat, df_konut, df_konut_faiz, df_tufe, df_mapping):
    X_all = pd.DataFrame()

    for ma in ma_list:
        X_all['input'+str(ma)] = monthly_sales[sku].rolling(window=ma).mean().shift(shift_value).values
    

    X_all.index = monthly_sales.index
    
    
    
    X_gaz = pd.DataFrame()
    X_dolar = pd.DataFrame()
    X_ruhsat = pd.DataFrame()
    X_konut = pd.DataFrame()
    X_konutfaiz = pd.DataFrame()
    X_tufe = pd.DataFrame()
    for ma in ma_list:
        X_gaz['gaz'+str(ma)] = df_gaz['TL/kwh'].rolling(window=ma).mean().shift(shift_value).values
        X_dolar['dolar'+str(ma)] = df_dolar['TP DK USD A YTL'].rolling(window=ma).mean().shift(shift_value).values
        X_ruhsat['ruhsat'+str(ma)] = df_ruhsat['Toplam_alan'].rolling(window=ma).mean().shift(shift_value).values
        X_konut['konut'+str(ma)] = df_konut['Konut Satış'].rolling(window=ma).mean().shift(shift_value).values
        X_konutfaiz['faiz'+str(ma)] = df_konut_faiz['Konut Kredisi Faiz Oranları'].rolling(window=ma).mean().shift(shift_value).values
        X_tufe['tufe'+str(ma)] = df_tufe['value'].rolling(window=ma).mean().shift(shift_value).values
    
    X_gaz.index = df_gaz.index
    X_dolar.index = df_dolar.index
    X_ruhsat.index = df_ruhsat.index
    X_konut.index = df_konut.index
    X_konutfaiz.index = df_konut_faiz.index
    X_tufe.index = df_tufe.index
    
    
    X_all = X_all.merge(X_gaz, how='left', left_index = True, right_index = True)
    X_all = X_all.merge(X_dolar, how='left', left_index = True, right_index = True)
    X_all = X_all.merge(X_konut, how='left', left_index = True, right_index = True)
    X_all = X_all.merge(X_konutfaiz, how='left', left_index = True, right_index = True)
    X_all = X_all.merge(X_ruhsat, how='left', left_index = True, right_index = True)
    X_all = X_all.merge(X_tufe, how='left', left_index = True, right_index = True)
    if len(df_mapping[df_mapping['sku'] == sku]) == 0:
        encoder_value = -1
    else:
        encoder_value = df_mapping[df_mapping['sku'] == sku].iloc[0,1]
    X_all['encoder_value'] = encoder_value
    
    return X_all
