
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import io
import joblib
connection_string = "DefaultEndpointsProtocol=https;AccountName=ksforecasting;AccountKey=LZqeSeeeOMz4OHg9mxZDOcr+GB4CunATrVOJPXzSEbCCAQlHMVSwdAztj/B6ADdcuG0j+gAxWJ6O+ASt1OEUAQ==;EndpointSuffix=core.windows.net"
    
container_name = "kstalep"

def read_blob_file(file_name, index_col=None):
  
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)
    
    blob_client_energy = container_client.get_blob_client(file_name)
    blob_data_energy = blob_client_energy.download_blob()
    csv_data_energy = blob_data_energy.readall()
    df = pd.read_csv(io.BytesIO(csv_data_energy),index_col=index_col)
    #df = pd.read_csv(file_name,index_col=index_col)
    return df

def read_joblib_file(filename):
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Download the blob (Joblib file)
        blob_client = container_client.get_blob_client(filename)  # Use filename here
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)

        # Load the model from the in-memory stream
        stream.seek(0)  # Move to the start of the stream
        model = joblib.load(stream)

        return model

    except Exception as e:
        print(f"Error: {e}")
        return None    

def write_blob_file(df, file_name):
    """
    Pandas DataFrame'i CSV formatında Azure Blob Storage'a yükler.

    :param df: Yüklenecek Pandas DataFrame.
    :param blob_name: Blob içine kaydedilecek dosyanın adı (örn. 'fiyat_dolar_dfu_perakende.csv').
    :param connection_string: Azure Blob Storage bağlantı dizesi.
    :param container_name: Blob Storage'daki konteyner adı.
    """
    try:
        # DataFrame'i CSV formatına dönüştür
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Blob servis client'ı oluştur
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Konteynere bağlan
        container_client = blob_service_client.get_container_client(container_name)
        
        # Blob client oluştur ve CSV verisini yükle
        blob_client = container_client.get_blob_client(file_name)
        
        # Veriyi yükle, overwrite=True ile aynı dosya varsa üzerine yazılır
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

        print(f"{file_name} başarıyla {container_name} konteynerine yüklendi.")
        
    except Exception as e:
        print(f"Hata oluştu: {e}")
