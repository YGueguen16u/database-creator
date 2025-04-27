from s3.s3_manager import upload_to_s3, list_s3_files

# Upload d’un fichier
upload_to_s3("data/processed/foods_cleaned.csv", "nutrition/foods_cleaned.csv")

# Lister les fichiers dans un dossier du bucket
list_s3_files("nutrition/")