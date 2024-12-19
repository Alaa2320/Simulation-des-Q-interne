import mysql.connector
import re
from conx import create_connection
import datetime


def execute_query(conn, query):
    """Execute the SQL query and return the result."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            result = cursor.fetchall()
            return result
        else:
            return f"{cursor.rowcount} lignes affectées."
    except mysql.connector.Error as err:
        return f"Erreur MySQL : {err}"
    except Exception as e:
        return f"Erreur inattendue : {e}"
    finally:
        if cursor:
            cursor.close()

def generate_internal_query(user_query):
    """Générer la requête interne à partir de la requête SQL de l'utilisateur."""
   
    internal_queries = []  # Initialiser la liste pour stocker les requêtes internes
    insert_foreign_key_query = ""

    conn = create_connection()
    if not conn:
        return "Erreur : Impossible de se connecter à la base de données des métadonnées."
    cursor = None
    try:
        cursor = conn.cursor()
  
        print(f"Received query: {user_query}")

        if user_query.lower().startswith("create table"):
            match = re.match(r"CREATE TABLE `?(\w+)\.`?(\w+)`?\s?\((.+)\)", user_query, re.IGNORECASE)
            if match:
                db_name = match.group(1).upper()  # Database name
                table_name = match.group(2).upper()  # Table name
                columns = match.group(3).split(",")  # List of columns
                

                check_db_query = f'SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}";'
                cursor.execute(check_db_query)
                db_id_result = cursor.fetchone()

                if not db_id_result:
                    return f"Erreur : La base de données '{db_name}' n'existe pas dans les métadonnées."
                db_id = db_id_result[0]
                # Vérifier si la table existe déjà dans la base de données
                check_table_query = f'SELECT COUNT(*) FROM General_TABLE_Tables WHERE table_name = "{table_name}" AND fk_db_id = {db_id};'
                cursor.execute(check_table_query)
                table_exists = cursor.fetchone()[0]

                if table_exists > 0:
                  return f"Erreur : La table '{table_name}' existe déjà dans la base de données '{db_name}'."


                # Add table to General_TABLE_Tables
                insert_table_query = f'''
                    INSERT INTO General_TABLE_Tables (table_name, fk_db_id, timestamp_insert)
                    VALUES ("{table_name}", {db_id}, NOW());
                '''
                cursor.execute(insert_table_query)
                conn.commit()
                internal_queries.append(insert_table_query)

                get_table_id_query = f'SELECT table_id FROM General_TABLE_Tables WHERE table_name = "{table_name}" AND fk_db_id = {db_id};'
                cursor.execute(get_table_id_query)
                table_id_result = cursor.fetchone()
           

                if not table_id_result:
                    return "Erreur : Impossible de récupérer l'ID de la table nouvellement créée."
                table_id = table_id_result[0]
                
                # Add columns to General_ATTRIBUTE_Tables
                for column in columns:
                    column_details = column.strip().split()
                    column_name = column_details[0] 
                   

                    
                    # Extract only the data type (e.g., INT, VARCHAR, etc.)
                    column_type = " ".join(
                          detail for detail in column_details[1:]
                           if detail.upper() not in ["PRIMARY", "KEY", "NOT", "NULL","FOREIGN","REFERENCES"]
                    ) 
                    is_nullable = "NOT NULL" not in " ".join(column_details).upper()

                    is_primary_key = "PRIMARY KEY" in " ".join(column_details).upper()

                    is_foreign_key = "FOREIGN KEY" in " ".join(column_details).upper() 


                    # Insert column attributes into General_ATTRIBUTE_Tables
                    insert_attribute_query = f'''
                        INSERT INTO General_ATTRIBUTE_Tables (
                            fk_table_id,
                            attribute_name, 
                            data_type, 
                            is_nullable, 
                            is_primary_key,
                            is_foreign_key,  
                            timestamp_insert
                        ) 
                        VALUES (
                            {table_id},
                            "{column_name}", 
                            "{column_type}" ,
                            {1 if is_nullable else 0}, 
                            {1 if is_primary_key else 0}, 
                            {1 if is_foreign_key else 0}, 
                            NOW()
                        );
                    '''
                    cursor.execute(insert_attribute_query)
                    conn.commit()
                    internal_queries.append(insert_attribute_query)

                    # Add PRIMARY KEY constraint if necessary
                    if is_primary_key:
                        constraint_name = f"PK_{table_name}_{column_name}"
                        insert_constraint_query = f'''
                            INSERT INTO General_CONSTRAINT_Tables (
                                constraint_name, 
                                fk_table_id, 
                                fk_attribute_id, 
                                constraint_type, 
                                timestamp_insert
                            )
                            VALUES (
                                "{constraint_name}", 
                                {table_id}, 
                                (SELECT attribute_id FROM General_ATTRIBUTE_Tables WHERE attribute_name = "{column_name}" AND fk_table_id = {table_id}), 
                                "PRIMARY_KEY", 
                                NOW()
                            );
                        '''
                        cursor.execute(insert_constraint_query)
                        conn.commit()
                        internal_queries.append(insert_constraint_query)

                    if is_foreign_key :
                        foreign_key_match = re.search(r"FOREIGN KEY \((\w+)\) REFERENCES (\w+)\.(\w+)\((\w+)\)", " ".join(column_details), re.IGNORECASE)
                        if foreign_key_match:
                            local_column_name = foreign_key_match.group(1)  # Colonne locale
                            referenced_db_name = db_name  # La base de données actuelle
                            referenced_table_name = foreign_key_match.group(3).upper()  # Table référencée
                            referenced_column_name = foreign_key_match.group(4)  # Colonne référencée
                             

        # Vérification de l'existence de la table référencée dans la base de données
                            check_referenced_table_query = f'''
                                      SELECT table_id 
                            FROM General_TABLE_Tables 
                            WHERE table_name = "{referenced_table_name}" 
                            AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{referenced_db_name}");
        '''
                            cursor.execute(check_referenced_table_query)
                            referenced_table_id_result = cursor.fetchone()
  # Utilisez fetchone() ici pour une seule ligne

                            if not referenced_table_id_result:
                                     return f"Erreur : La table référencée '{referenced_table_name}' n'existe pas."

                            referenced_table_id = referenced_table_id_result[0]
                   

        # Vérification de l'existence de la colonne référencée dans la table référencée
                            check_referenced_column_query = f'''
                                    SELECT attribute_id 
                                    FROM General_ATTRIBUTE_Tables 
                                     WHERE attribute_name = "{referenced_column_name}" 
                                        AND fk_table_id = {referenced_table_id};
        '''
                            cursor.execute(check_referenced_column_query)
                            referenced_attribute_id_result = cursor.fetchone() 

                            if not referenced_attribute_id_result:
                                  return f"Erreur : La colonne référencée '{referenced_column_name}' dans la table '{referenced_table_name}' n'existe pas dans les métadonnées."

                            referenced_attribute_id = referenced_attribute_id_result[0]  # Récupération de l'ID de la colonne référencée

        # Insertion de la contrainte FOREIGN KEY dans General_CONSTRAINT_Tables
                            constraint_name = f"FK_{table_name}_{local_column_name}"
                            insert_foreign_key_query = f'''
                                    INSERT INTO General_CONSTRAINT_Tables (
                                    constraint_name, 
                                    fk_table_id, 
                                    fk_attribute_id, 
                                    constraint_type, 
                                    fk_referenced_table_id, 
                                    fk_referenced_attribute_id, 
                                    timestamp_insert
                                         )
                            VALUES (
                                    "{constraint_name}", 
                                     {table_id}, 
                                    (SELECT attribute_id 
                                     FROM General_ATTRIBUTE_Tables 
                                      WHERE attribute_name = "{local_column_name}" 
                                     AND fk_table_id = {table_id}),
                                     "FOREIGN_KEY", 
                                     {referenced_table_id},
                                    
                                     {referenced_attribute_id},
                                      NOW()
                                                      );
                   '''
                    cursor.execute(insert_foreign_key_query)
                    conn.commit()
                    internal_queries.append(insert_foreign_key_query)

 
                    
                return "\n".join(internal_queries)

            else:
                return "Erreur : Le format de la requête CREATE TABLE est incorrect."
            
#
        # Gérer la requête SELECT * FROM bdd.tab
        if user_query.lower().startswith("select * from"):
            match = re.match(r"SELECT \* FROM `?(\w+)`?\.`?(\w+)`?", user_query, re.IGNORECASE)
            if match:
                db_name = match.group(1).upper()  # Nom de la base de données
                table_name = match.group(2).upper()  # Nom de la table

                # Vérification si la table existe dans la base de données
                check_table_query = f'''
                    SELECT table_id 
                    FROM General_TABLE_Tables 
                    WHERE table_name = "{table_name}" 
                    AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}");
                    LIMIT 1;
                '''
                cursor.execute(check_table_query)
                table_result = cursor.fetchone()
               
                # Requête interne pour SELECT * FROM bdd.tab
                internal_query = f"SELECT * FROM `{db_name}`.`{table_name}`;"
                internal_queries.append(internal_query)
                
        # Affichage de la requête interne générée
            return "Requête Interne Générée :\n" + "\n".join(internal_queries)
          
# rename table
         
        if user_query.lower().startswith("rename table"):
            match = re.match(r"RENAME TABLE `?(\w+)\.`?(\w+)`? TO `?(\w+)\.`?(\w+)`?", user_query, re.IGNORECASE)
            if match:
                old_db_name = match.group(1).upper()  # Nom de l'ancienne base de données
                old_table_name = match.group(2).upper()  # Nom de l'ancienne table
                new_db_name = match.group(3).upper()  # Nom de la nouvelle base de données
                new_table_name = match.group(4).upper()  # Nom de la nouvelle table

            # Vérifier si la table existe dans les métadonnées
                check_table_query = f'''
                SELECT table_id, fk_db_id FROM General_TABLE_Tables
                WHERE table_name = "{old_table_name}" 
                AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{old_db_name}");
            '''
                cursor.execute(check_table_query)
                table_result = cursor.fetchone()
                if not table_result:
                  return f"Erreur : La table '{old_table_name}' n'existe pas dans la base de données '{old_db_name}'."

                table_id, old_db_id = table_result

            # Vérifier si la nouvelle base de données existe
                check_new_db_query = f'SELECT db_id FROM General_BD_Tables WHERE db_name = "{new_db_name}";'
                cursor.execute(check_new_db_query)
                new_db_result = cursor.fetchone()
                if not new_db_result:
                   return f"Erreur : La base de données '{new_db_name}' n'existe pas."

                new_db_id = new_db_result[0]

            # Mettre à jour le nom de la table et la base de données associée
                update_table_query = f'''
                UPDATE General_TABLE_Tables 
                SET table_name = "{new_table_name}", fk_db_id = {new_db_id}
                WHERE table_id = {table_id}  ;
            '''
                cursor.execute(update_table_query)
                conn.commit()
                internal_queries.append(update_table_query)

                return (f"Requête Interne Générée :\n{update_table_query}\n\n"
                    f"La table '{old_table_name}' a été renommée en '{new_table_name}' dans la base de données '{new_db_name}'.")
         
         #ajouter un column 
        if user_query.lower().startswith("alter table") and "add column" in user_query.lower():
              match = re.match(r"ALTER TABLE `?(\w+)\.`?(\w+)`? ADD COLUMN `?(\w+)`? (\w+(\(\d+\))?)", user_query, re.IGNORECASE)
              if match:
                   db_name = match.group(1).upper()  # Nom de la base de données
                   table_name = match.group(2).upper()  # Nom de la table
                   column_name = match.group(3)  # Nom de la colonne à ajouter
                   data_type = match.group(4).upper()  # Type de données de la colonne

        # Vérifiez si la table existe dans les métadonnées
                   check_table_query = f'''
            SELECT table_id FROM General_TABLE_Tables 
            WHERE table_name = "{table_name}" 
            AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}");
        '''
                   cursor.execute(check_table_query)
                   table_result = cursor.fetchone()
                   if not table_result:
                         return f"Erreur : La table '{table_name}' n'existe pas dans la base de données '{db_name}'."

                   table_id = table_result[0]

        # Ajouter la colonne dans les métadonnées
                   insert_column_query = f'''
            INSERT INTO General_ATTRIBUTE_Tables (
                fk_table_id,
                attribute_name, 
                data_type, 
                is_nullable, 
                is_primary_key,
                is_foreign_key,  
                timestamp_insert
            ) 
            VALUES (
                {table_id},
                "{column_name}", 
                "{data_type}", 
                1,  
                0,  
                0,  
                NOW()
            );
        '''
                   cursor.execute(insert_column_query)
                   conn.commit()
                   internal_queries.append(insert_column_query)
                   return (
        f"Requête Interne Générée :\n{insert_column_query}\n\n"
        f"Colonne '{column_name}' ajoutée avec succès à la table '{table_name}' avec le type '{data_type}'."
    )
              #modify
        if user_query.lower().startswith("alter table") and "modify column" in user_query.lower():
            match = re.match(r"ALTER TABLE `?(\w+)\.`?(\w+)`? MODIFY COLUMN `?(\w+)`? (\w+(\(\d+\))?)", user_query, re.IGNORECASE)
            if match:
             db_name = match.group(1).upper()  # Nom de la base de données
             table_name = match.group(2).upper()  # Nom de la table
             column_name = match.group(3).lower()  # Nom de la colonne à modifier
             new_data_type = match.group(4).upper()  # Nouveau type de données

        # Vérifiez si la colonne existe dans les métadonnées
            check_column_query = f'''
            SELECT attribute_id FROM General_ATTRIBUTE_Tables 
            WHERE attribute_name = "{column_name}" 
            AND fk_table_id = (
                SELECT table_id FROM General_TABLE_Tables 
                WHERE table_name = "{table_name}" 
                AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}")
            );
        '''
            cursor.execute(check_column_query)
            column_result = cursor.fetchone()
            if not column_result:
               return f"Erreur : La colonne '{column_name}' n'existe pas dans la table '{table_name}'."

            column_id = column_result[0]

        # Modifiez la colonne dans les métadonnées
            modify_column_query = f'''
            UPDATE General_ATTRIBUTE_Tables
            SET data_type = "{new_data_type}"
            WHERE attribute_id = {column_id};
        '''
            cursor.execute(modify_column_query)
            conn.commit()

            internal_queries.append(modify_column_query)
            return (
                       f"Requête Interne Générée :\n{modify_column_query}\n\n"
                       f"Colonne '{column_name}' modifiée avec succès dans la table '{table_name}' avec le type '{new_data_type}'."
    )
        #drop cloumn
        if user_query.lower().startswith("alter table") and "drop column" in user_query.lower():
            match = re.match(r"ALTER TABLE `?(\w+)\.`?(\w+)`? DROP COLUMN `?(\w+)`?", user_query, re.IGNORECASE)
            if match:
             db_name = match.group(1).upper()  # Nom de la base de données
             table_name = match.group(2).upper()  # Nom de la table
             column_name = match.group(3).lower()  # Nom de la colonne à supprimer

        # Vérifiez si la colonne existe dans les métadonnées
            check_column_query = f'''
            SELECT attribute_id FROM General_ATTRIBUTE_Tables 
            WHERE attribute_name = "{column_name}" 
            AND fk_table_id = (
                SELECT table_id FROM General_TABLE_Tables 
                WHERE table_name = "{table_name}" 
                AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}")
            );
        '''
            cursor.execute(check_column_query)
            column_result = cursor.fetchone()
            if not column_result:
               return f"Erreur : La colonne '{column_name}' n'existe pas dans la table '{table_name}'."

            column_id = column_result[0]
            # Vérifier si la colonne fait partie d'une clé primaire et supprimer la contrainte PK

            check_pk_query = f'''
              SELECT constraint_id FROM General_CONSTRAINT_Tables
                WHERE fk_attribute_id = {column_id}
                 AND constraint_type = 'PRIMARY_KEY';
                '''
            cursor.execute(check_pk_query)
            pk_result = cursor.fetchone()
            if pk_result:
              pk_constraint_id = pk_result[0]
            # Supprimer la contrainte PRIMARY KEY
              delete_pk_query = f'DELETE FROM General_CONSTRAINT_Tables WHERE constraint_id = {pk_constraint_id};'
              cursor.execute(delete_pk_query)
              internal_queries.append(delete_pk_query)
              conn.commit()

        # Supprimez la colonne des métadonnées
            delete_column_query = f'DELETE FROM General_ATTRIBUTE_Tables WHERE attribute_id = {column_id};'
            cursor.execute(delete_column_query)
            conn.commit()
            internal_queries.append(delete_column_query)
            return (
                         f"Requête Interne Générée :\n"+"\n".join(internal_queries) +
                       f"Colonne '{column_name}' supprimée avec succès de la table '{table_name}'."
    )
    #drop table
        if user_query.lower().startswith("drop table"):
          match = re.match(r"DROP TABLE `?(\w+)\.`?(\w+)`?", user_query, re.IGNORECASE)
          if match:
           db_name = match.group(1).upper()  # Nom de la base de données
           table_name = match.group(2).upper()  # Nom de la table

        # Vérifiez si la table existe dans les métadonnées
          check_table_query = f'''
            SELECT table_id FROM General_TABLE_Tables 
            WHERE table_name = "{table_name}" 
            AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}");
        '''
          cursor.execute(check_table_query)
          table_result = cursor.fetchone()
          if not table_result:
            return f"Erreur : La table '{table_name}' n'existe pas dans la base de données '{db_name}'."

          table_id = table_result[0]
        # Supprimer les contraintes associées

          delete_constraints_query = f'DELETE FROM General_CONSTRAINT_Tables WHERE fk_table_id = {table_id};'
          cursor.execute(delete_constraints_query)
          conn.commit()
          internal_queries.append(delete_constraints_query)
        # Supprimer les attributs associés

          delete_attributes_query = f'DELETE FROM General_ATTRIBUTE_Tables WHERE fk_table_id = {table_id};'
          cursor.execute(delete_attributes_query)
          conn.commit()
          internal_queries.append(delete_attributes_query)
                  # Supprimer la table des métadonnées
          delete_table_query = f'DELETE FROM General_TABLE_Tables WHERE table_id = {table_id};'
          cursor.execute(delete_table_query)
          conn.commit()
          internal_queries.append(delete_table_query)
          return (
            f"Requête Interne Générée :\n"
            f"{delete_constraints_query}\n"
            f"{delete_attributes_query}\n"
            f"{delete_table_query}\n\n"
            f"Table '{table_name}' supprimée avec succès de la base de données '{db_name}'."
        )


        # Gestion de la création d'une base de données
        if user_query.lower().startswith("create database"):
            # Extraire le nom de la base de données avec une expression régulière
            match = re.search(r"CREATE DATABASE `?(\w+)`?", user_query, re.IGNORECASE)
            if match:
                db_name = match.group(1).upper()  # Convertir en majuscule pour éviter les doublons
                
                # Vérifier si la base existe déjà dans les métadonnées
                check_query = f'SELECT COUNT(*) FROM General_BD_Tables WHERE db_name = "{db_name}";'
                cursor.execute(check_query)
                result = cursor.fetchone()
                

                if result[0] > 0:
                    return f"Erreur : La base de données '{db_name}' existe déjà dans les métadonnées."

                # Ajouter la base de données dans `General_BD_Tables`
                internal_query = f'INSERT INTO General_BD_Tables (db_name, timestamp_insert) VALUES ("{db_name}", NOW());'
                cursor.execute(internal_query)
                conn.commit()
                return f"Requête Interne Générée :\n{internal_query}\n\nLa base de données '{db_name}' a été créée avec succès."

            else:
                return "Erreur : Le format de la requête CREATE DATABASE est incorrect."

        # Gestion de la suppression d'une base de données
        elif user_query.lower().startswith("drop database"):
            db_name = user_query.split()[2].strip(';').upper()
            check_query = f'SELECT COUNT(*) FROM General_BD_Tables WHERE db_name = "{db_name}";'
            cursor.execute(check_query)
            result = cursor.fetchone()

            if result[0] > 0:
                # Supprimer la base de données des métadonnées
                internal_query = f'DELETE FROM General_BD_Tables WHERE db_name = "{db_name}";'
                cursor.execute(internal_query)
                conn.commit()
                return f"Requête Interne Générée :\n{internal_query}\n\nLa base de données '{db_name}' a été supprimée avec succès."

            else:
                return f"Erreur : La base de données '{db_name}' n'existe pas dans les métadonnées."
            
        #renemage de base de donne
        if user_query.lower().startswith("rename database"):
           match = re.match(r"RENAME DATABASE `?(\w+)`? TO `?(\w+)`?", user_query, re.IGNORECASE)
           if match:
            old_db_name = match.group(1).upper()  # Nom de l'ancienne base de données
            new_db_name = match.group(2).upper()  # Nom de la nouvelle base de données

    # Vérifier si l'ancienne base de données existe dans les métadonnées
            check_old_db_query = f'''
           SELECT db_id FROM General_BD_Tables
            WHERE db_name = "{old_db_name}";
        '''
           cursor.execute(check_old_db_query)
           old_db_result = cursor.fetchone()
           if not old_db_result:
               return f"Erreur : La base de données '{old_db_name}' n'existe pas."

           old_db_id = old_db_result[0]
    # Vérifier si la nouvelle base de données existe déjà dans les métadonnées
           check_new_db_query = f'''
        SELECT db_id FROM General_BD_Tables WHERE db_name = "{new_db_name}";
        '''
           cursor.execute(check_new_db_query)
           new_db_result = cursor.fetchone()
           if new_db_result:
              return f"Erreur : La base de données '{new_db_name}' existe déjà."

    # Mettre à jour le nom de la base de données dans les métadonnées
           update_db_query = f'''
        UPDATE General_BD_Tables
        SET db_name = "{new_db_name}"
        WHERE db_id = {old_db_id};
        '''
           cursor.execute(update_db_query)

        
           conn.commit()
           internal_queries.append(update_db_query)

           return (f"Requête Interne Générée :\n{update_db_query}\n"
                   f"La base de données '{old_db_name}' a été renommée en '{new_db_name}'.")
        #inset into
        # Vérifier si la requête commence par INSERT INTO
        if user_query.lower().startswith("insert into"):
            match = re.match(r"INSERT INTO `?(\w+)\.`?(\w+)`?\s?\((.+)\)\s?VALUES\s?\((.+)\)", user_query, re.IGNORECASE)
            if match:
             db_name = match.group(1).upper()  # Nom de la base de données
             table_name = match.group(2).upper()  # Nom de la table
             columns = match.group(3).split(",")  # Colonnes à insérer
             values = match.group(4).split(",")  # Valeurs à insérer

            # Vérification que le nombre de valeurs correspond au nombre de colonnes
             if len(columns) != len(values):
                return f"Erreur : Le nombre de colonnes ({len(columns)}) ne correspond pas au nombre de valeurs ({len(values)})."

            # Nettoyage des espaces superflus dans les colonnes et les valeurs
             columns = [col.strip() for col in columns]
             values = [val.strip() for val in values]

            # Vérification des types de données pour chaque colonne
             column_types_query = f'''
                SELECT attribute_name, data_type, attribute_id FROM General_ATTRIBUTE_Tables
                WHERE fk_table_id = (
                    SELECT table_id FROM General_TABLE_Tables
                    WHERE table_name = "{table_name}" 
                    AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}")
                );
            '''
             cursor.execute(column_types_query)
             column_types_result = cursor.fetchall()

             if len(column_types_result) != len(columns):
                return f"Erreur : Le nombre de colonnes dans les métadonnées ne correspond pas au nombre de colonnes spécifiées."

            # Insertion dans General_VALUE_Tables
             for i, (col, value) in enumerate(zip(columns, values)):
                # Récupérer l'ID de la colonne
                column_info = column_types_result[i]
                column_id = column_info[2]  # L'ID de l'attribut de la colonne
                col_type = column_info[1].upper()

                # Gestion des types de données (ex: INT, VARCHAR, etc.)
                if col_type == "INT" and not value.isdigit():
                    return f"Erreur : La valeur '{value}' pour la colonne '{col}' doit être un entier."
                elif col_type == "VARCHAR" and len(value) > 255:
                    return f"Erreur : La valeur '{value}' pour la colonne '{col}' dépasse la longueur maximale autorisée (255 caractères)."

                # Préparer l'insertion dans General_VALUE_Tables
                insert_value_query = f'''
                    INSERT INTO General_VALUE_Tables (fk_table_id, fk_attribute_id, value_text)
                    VALUES (
                        (SELECT table_id FROM General_TABLE_Tables WHERE table_name = "{table_name}" AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}")),
                        {column_id},
                        "{value}"
                    );
                '''
                cursor.execute(insert_value_query)
                internal_queries.append(insert_value_query)
                conn.commit()

            return (
                f"Requêtes internes générées :\n" +
                "\n".join(internal_queries) +
                f"\n\nLes valeurs ont été insérées avec succès dans la table 'General_VALUE_Tables'."
            )


        # Affichage des bases de données
        elif user_query.lower().startswith("show databases"):
            internal_query = "SELECT db_name FROM General_BD_Tables ORDER BY timestamp_insert DESC;"
            return(f"Requéte Interne Générée :\n\n{internal_query}.")

        elif user_query.lower().startswith("show tables"):
            internal_query = """
                SELECT db_name, table_name
                FROM General_BD_Tables 
                JOIN General_TABLE_Tables ON General_BD_Tables.db_id = General_TABLE_Tables.fk_db_id
                ORDER BY db_name, table_name;

            """
            return(f"Requéte Interne Générée:\n{internal_query}")
        else:
         return "Erreur : Requête non reconnue ou non supportée."

    except mysql.connector.Error as err:
     return f"Erreur MySQL lors de la génération de la requête : {err}"
    except Exception as e:
     return f"Erreur inattendue lors de la génération de la requête : {e}"
    finally:
      conn.close()

    return internal_query 
#


#

def audit_sql_query(user, query_text):
    """
    Enregistre une requête SQL exécutée dans la table General_SQL_Audit.

    :param user_id: ID de l'utilisateur ayant exécuté la requête
    :param query_text: Requête SQL exécutée
    """
    conn = create_connection()
    if not conn:
        raise ValueError("Erreur : Impossible de se connecter à la base de données.")

    try:
        cursor = conn.cursor()
        audit_query = """
            INSERT INTO General_Audit_logs (user, query_text, timestamp)
            VALUES (%s, %s, NOW());
        """
        cursor.execute(audit_query, (user, query_text))
        conn.commit()
    except Exception as e:
        print(f"Erreur lors de l'enregistrement de la requête SQL dans l'audit : {str(e)}")
    finally:
        conn.close()


#



def handle_query(user,user_query):
    """Gérer l'exécution de la requête SQL de l'utilisateur et afficher la requête interne."""
    conn = create_connection()
    if not conn:

        return "Erreur : Impossible de se connecter à la base de données."

    try:

        cursor = conn.cursor()
        # Générer la requête interne à partir de la requête SQL de l'utilisateur
        internal_query = generate_internal_query(user_query)

        # Afficher la requête interne générée pour vérifier
        print(f"Requête interne générée : {internal_query}")
        # Enregistrer la requête dans l'audit
        audit_sql_query(user, user_query)


        # Si une erreur est détectée dans la génération de la requête interne, on l'affiche
        if internal_query.startswith("Erreur"):
            return internal_query

       
        # Exécuter la requête interne
        result = execute_query(conn, internal_query)


        
        # Retourner l'entrée SQL, la requête interne et son résultat
        return (
            f"\n{internal_query}\n\n"
           
        )

    except mysql.connector.Error as err:
        return f"Erreur MySQL lors de l'exécution de la requête : {err}"
    except Exception as e:
        return f"Erreur inattendue lors du traitement de la requête : {str(e)}"
    finally:
        if cursor:
            cursor.close()  # Always close the cursor when done
        if conn:
            conn.close()  # Always close the connection when done



#
def get_tables_for_db(db_name):
    """Récupérer la liste des tables associées à une base de données."""
    conn = create_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT table_name 
            FROM General_TABLE_Tables 
            WHERE fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}");
        ''')
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except mysql.connector.Error as err:
        print(f"Erreur MySQL : {err}")
        return []
    finally:
        conn.close()
        #

def get_databases():
    """Récupérer la liste des bases de données depuis les métadonnées."""
    conn = create_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT db_name FROM General_BD_Tables ORDER BY timestamp_insert DESC;")
        result = cursor.fetchall()
        return [row[0] for row in result]
    except mysql.connector.Error as err:
        print(f"Erreur MySQL : {err}")
        return []
    finally:
        conn.close()

def get_table_columns(table_name):
    # Créer la requête pour obtenir les colonnes de la table
    query = f"DESCRIBE {table_name};"
    # Passer la requête à execute_query
    columns = execute_query(query)
    return columns
 
def get_table_data(db_name, table_name):
    """Retourne les données d'une table spécifique dans une base de données."""
    query = f"SELECT * FROM `{db_name}`.`{table_name}`"
    result = execute_query(query)
    return result
 # Cette fonction exécutera la requête SQL et retournera les résultats

 




def get_attributes_for_table(db_name, table_name):
    """
    Récupère les attributs de la table spécifiée dans la base de données donnée.
    :param db_name: Le nom de la base de données
    :param table_name: Le nom de la table
    :return: Une liste d'attributs avec leurs informations
    """
    # Utiliser la fonction de connexion
    conn = create_connection()
    cursor = conn.cursor()

    # Requête SQL pour obtenir les attributs de la table
    query = f"""
    SELECT attribute_name, data_type, is_primary_key, is_foreign_key
    FROM General_ATTRIBUTE_Tables
    WHERE fk_table_id = (SELECT table_id FROM General_TABLE_Tables WHERE table_name = '{table_name}' AND fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = '{db_name}'))
    """
    
    # Exécution de la requête et récupération des résultats
    cursor.execute(query)
    attributes = cursor.fetchall()
    
    # Retourner les attributs sous forme de liste de dictionnaires
    attribute_list = []
    for row in attributes:
        attribute = {
            "name": row[0],
            "type": row[1],
            "is_primary_key": bool(row[2]),
            "is_foreign_key": bool(row[3])
        }
        attribute_list.append(attribute)
    
    cursor.close()
    conn.close()

    return attribute_list
#

#
def refresh_dashboard(db_name=None):
    """Rafraîchir le tableau de bord pour afficher les bases de données et leurs tables."""
    conn = create_connection()
    if not conn:
        return "Erreur : Impossible de se connecter à la base de données."

    try:
        cursor = conn.cursor()

        # Si une base de données est spécifiée, afficher ses tables
        if db_name:
            cursor.execute(f'SELECT table_name FROM General_TABLE_Tables WHERE fk_db_id = (SELECT db_id FROM General_BD_Tables WHERE db_name = "{db_name}");')
            tables = cursor.fetchall()
            return tables
        else:
            # Récupérer la liste des bases de données dans General_BD_Tables
            cursor.execute("SELECT db_name FROM General_BD_Tables ORDER BY timestamp_insert DESC;")
            databases = cursor.fetchall()
            return databases

    except Exception as e:
        return f"Erreur lors du rafraîchissement du tableau de bord : {str(e)}"
    finally:
        conn.close()




