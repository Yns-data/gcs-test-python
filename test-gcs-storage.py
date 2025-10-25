from google.cloud import storage
import pandas as pd
from io import BytesIO

# ⚙️ Configuration
BUCKET_NAME = "mon-bucket"
SOURCE_BLOB_NAME = "data/input.csv"     # chemin du CSV source dans le bucket
DESTINATION_BLOB_NAME = "data/output.csv"  # chemin du CSV modifié

def read_csv_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
    """Télécharge un CSV depuis un bucket GCS et le charge dans un DataFrame pandas."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    data = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(data))
    print(f"✅ CSV '{blob_name}' lu avec {len(df)} lignes.")
    return df

def write_csv_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str):
    """Écrit un DataFrame pandas vers un fichier CSV dans GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Conversion du DataFrame en bytes
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload vers GCS
    blob.upload_from_file(csv_buffer, content_type="text/csv")
    print(f"✅ CSV modifié uploadé vers '{blob_name}'.")

def modify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Exemple simple de modification : ajout d'une colonne calculée."""
    df["new_column"] = df[df.columns[0]].apply(lambda x: str(x).upper())  # exemple : majuscules
    print("🛠️ Données modifiées (colonne 'new_column' ajoutée).")
    return df

def main():
    df = read_csv_from_gcs(BUCKET_NAME, SOURCE_BLOB_NAME)
    df_modified = modify_dataframe(df)
    write_csv_to_gcs(df_modified, BUCKET_NAME, DESTINATION_BLOB_NAME)

if __name__ == "__main__":
    main()
