import pypff
import pandas as pd
from datetime import datetime
import os
from bs4 import BeautifulSoup

def extract_pst_to_xlsx(pst_file_path, output_xlsx):
    """
    Extraire les emails d'un fichier PST et les sauvegarder dans un fichier XLSX
    
    Args:
        pst_file_path (str): Chemin vers le fichier PST
        output_xlsx (str): Chemin pour le fichier XLSX de sortie
    """
    try:
        # Ouvrir le fichier PST
        pst = pypff.file()
        pst.open(pst_file_path)
        
        # Initialiser la liste pour stocker les données des emails
        emails_data = []
        
        def html_to_text(html_content):
            """Convertit le contenu HTML en texte brut"""
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                # Supprimer les balises script et style
                for script in soup(["script", "style"]):
                    script.decompose()
                # Récupérer le texte
                text = soup.get_text(separator=' ', strip=True)
                return text
            except:
                return html_content
        
        def get_message_body(message):
            """Essaie de récupérer le corps du message dans différents formats"""
            # Essayer d'abord le texte brut
            body = message.get_plain_text_body()
            if body:
                return body
                
            # Sinon essayer le HTML et le convertir en texte
            try:
                body = message.get_html_body()
                if body:
                    return html_to_text(body)
            except:
                pass
                
            # Enfin essayer le RTF
            try:
                body = message.get_rtf_body()
                if body:
                    return body  # On pourrait ajouter un convertisseur RTF->texte si nécessaire
            except:
                pass
                
            return ''
        
        def process_folder(folder):
            """Fonction récursive pour traiter les dossiers et sous-dossiers"""
            message_count = folder.get_number_of_sub_messages()
            
            # Parcourir tous les messages dans le dossier
            for i in range(message_count):
                message = folder.get_sub_message(i)
                
                # Extraire les informations essentielles du message
                email_data = {
                    'subject': message.get_subject() or '',
                    'sender': message.get_sender_name() or '',
                    'delivery_time': message.get_delivery_time() or '',
                    'body': get_message_body(message),
                    'folder': folder.get_name() or ''
                }
                
                emails_data.append(email_data)
            
            # Traiter les sous-dossiers
            for sub_folder in folder.sub_folders:
                process_folder(sub_folder)
        
        # Commencer le traitement depuis la racine
        root = pst.get_root_folder()
        process_folder(root)
        
        # Convertir en DataFrame pandas
        df = pd.DataFrame(emails_data)
        
        # Convertir le timestamp en format datetime lisible
        def convert_timestamp(x):
            if not x:
                return ''
            try:
                if isinstance(x, (int, float)):
                    return datetime.fromtimestamp(x)
                return x
            except:
                return x
        
        df['delivery_time'] = df['delivery_time'].apply(convert_timestamp)
        
        # Sauvegarder en XLSX
        df.to_excel(output_xlsx, index=False, engine='openpyxl')
        
        print(f"Conversion terminée. Fichier sauvegardé : {output_xlsx}")
        
    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")
        raise e
    finally:
        if 'pst' in locals():
            pst.close()

if __name__ == "__main__":
    # Exemple d'utilisation
    pst_file = "backup.pst"
    output_file = "emails_extraits.xlsx"
    
    if os.path.exists(pst_file):
        extract_pst_to_xlsx(pst_file, output_file)
    else:
        print(f"Le fichier PST n'existe pas : {pst_file}")
