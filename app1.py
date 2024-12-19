import tkinter as tk
from tkinter import scrolledtext, messagebox
from tkinter import ttk
from ttkthemes import ThemedTk  # Module pour appliquer des thèmes modernes
import re
import mysql.connector
import datetime
import bcrypt
from conx import create_connection

from trait import handle_query, get_databases, get_tables_for_db, get_attributes_for_table, get_table_data

def setup_gui():
    """Configurer une interface graphique moderne pour l'exécution des requêtes SQL et l'affichage des résultats."""
    window = ThemedTk(theme="arc")  # Utilisation d'un thème moderne
    window.geometry("1200x700")
    window.resizable(True, True)

    # Styles ttk
    style = ttk.Style()
    style.configure("TLabel", font=("Helvetica", 12))
    style.configure("TButton", font=("Helvetica", 12))
    style.configure("Treeview", font=("Helvetica", 10), rowheight=25)
    style.configure("Treeview.Heading", font=("Helvetica", 12, "bold"))

    # Titre
    title_label = ttk.Label(window, text="Simulateur de Requêtes Internes", anchor="center", font=("Helvetica", 18, "bold"))
    title_label.pack(fill="x", pady=10)

    # Frame principale
    main_frame = ttk.Frame(window, padding=10)
    main_frame.pack(fill="both", expand=True)

    # Tableau de bord
    dashboard_frame = ttk.Frame(main_frame, relief="ridge")
    dashboard_frame.pack(side="left", fill="y", padx=(0, 10), pady=10)
    ttk.Label(dashboard_frame, text="Bases de Données & Tables", anchor="center").pack(fill="x", pady=(5, 5))

    treeview = ttk.Treeview(dashboard_frame, selectmode="browse", show="tree")
    treeview.pack(fill="both", expand=True, padx=5, pady=5)

    # Zone de saisie des requêtes
    query_frame = ttk.LabelFrame(main_frame, text="Requête SQL", padding=10)
    query_frame.pack(fill="x", pady=(0, 10))

    query_entry = scrolledtext.ScrolledText(query_frame, height=5, wrap=tk.WORD, font=("Courier New", 10), relief="groove")
    query_entry.pack(fill="x", padx=5, pady=5)

    execute_button = ttk.Button(query_frame, text="Exécuter", command=lambda: on_execute())
    execute_button.pack(anchor="e", padx=5, pady=5)

    # Zone des résultats
    result_frame = ttk.Notebook(main_frame)
    result_frame.pack(fill="both", expand=True)

    internal_query_tab = ttk.Frame(result_frame, padding=10)
    result_tab = ttk.Frame(result_frame, padding=10)

    result_frame.add(internal_query_tab, text="Requête Interne")
    result_frame.add(result_tab, text="Résultats")

    internal_query_display = scrolledtext.ScrolledText(internal_query_tab, height=15, wrap=tk.WORD, font=("Courier New", 10), state="disabled")
    internal_query_display.pack(fill="both", expand=True)

    table_info_display = scrolledtext.ScrolledText(result_tab, height=15, wrap=tk.WORD, font=("Courier New", 10), state="disabled")
    table_info_display.pack(fill="both", expand=True)

    tk.Label(
    query_frame,
    text="Logs des Requêtes SQL :",
    font=("Helvetica", 12, "bold"),  # Taille de la police définie à 12
    anchor="w",
    bg="#ecf0f1"
).pack(fill="x", padx=10, pady=(10, 0))
    log_display = scrolledtext.ScrolledText(
    query_frame,
    height=8,
    wrap=tk.WORD,
    font=("Courier New", 10),  # Taille de la police définie à 10
    bg="#ffffff",
    fg="#2c3e50",
    bd=2,
    relief="solid",
    state="disabled"
)
    log_display.pack(fill="x", padx=30, pady=(15, 15))
    window = ThemedTk(theme="arc")

    def refresh_databases():
        """Mettre à jour la liste des bases de données dans le tableau de bord."""
        treeview.delete(*treeview.get_children())
        databases = get_databases()
        for db in databases:
            db_node = treeview.insert("", "end", text=db)
            tables = get_tables_for_db(db)
            for table in tables:
                treeview.insert(db_node, "end", text=table)

                #
    def display_sql_audit_logs():
      """
         Affiche les logs d'audit SQL dans une zone dédiée.
       """
    conn = create_connection()
    if not conn:
        messagebox.showerror("Erreur", "Impossible de se connecter à la base de données.")
        return

    try:
        cursor = conn.cursor()
        query = """
            SELECT a.timestamp, u.username, a.query_text
            FROM General_Audit_table a
            JOIN Users u ON a.user = u.username
            ORDER BY a.timestamp DESC;
        """
        cursor.execute(query)
        logs = cursor.fetchall()

        # Afficher les logs dans une zone de texte
        log_display.config(state="normal")
        log_display.delete("1.0", tk.END)
        for log in logs:
            timestamp, username, query_text = log
            log_display.insert(tk.END, f"[{timestamp}] {username} : {query_text}\n")
        log_display.config(state="disabled")
        log_display.update()
    finally:
        conn.close()

     #


    def display_table_info(db_name, table_name):
       

        """Afficher les attributs d'une table."""
        attributes = get_attributes_for_table(db_name, table_name)
        table_info = f"Table : {table_name}\n{'-'*50}\n"
        table_info += f"{'Attribute Name':<20}{'Data Type':<20}{'Primary Key':<15}{'Foreign Key':<15}\n"
        table_info += "-"*50 + "\n"
        for attr in attributes:
            table_info += f"{attr['name']:<20}{attr['type']:<20}{'Yes' if attr['is_primary_key'] else 'No':<15}{'Yes' if attr['is_foreign_key'] else 'No':<15}\n"

        table_info_display.config(state="normal")
        table_info_display.delete("1.0", tk.END)
        table_info_display.insert("1.0", table_info)
        table_info_display.config(state="disabled")

    
    
    

    def on_execute():
        """Gérer l'exécution des requêtes SQL via la fonction handle_query."""
        user_query = query_entry.get("1.0", tk.END).strip()
        user= "admin"
        if not user_query:
            messagebox.showwarning("Attention", "Veuillez saisir une requête SQL.")

            return

        try:


            result = handle_query(user , user_query)

            # Enregistrer le log d'audit
            # Enregistrer la requête dans les logs d'audit
            conn = create_connection()
            if conn:
             cursor = conn.cursor()
             timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
             cursor.execute("""
                INSERT INTO General_Audit_table(timestamp, user, query_text)
                VALUES (%s, %s, %s)
            """, (timestamp, user, user_query))
            conn.commit()
            conn.close()
            display_sql_audit_logs()

            
            internal_query_display.config(state="normal")
            internal_query_display.delete("1.0", tk.END)
            internal_query_display.insert("1.0", result)
            internal_query_display.config(state="disabled")

            # Si une requête CREATE/ALTER TABLE est exécutée, afficher les informations de la table
            if user_query.lower().startswith("create table") or user_query.lower().startswith("alter table"):
                match = re.match(r"(CREATE|ALTER) TABLE ?(\w+)\.?(\w+)", user_query, re.IGNORECASE)
                if match:
                    db_name = match.group(2).upper()
                    table_name = match.group(3).upper()
                    display_table_info(db_name, table_name)
            # Si une requête SELECT * FROM est exécutée, afficher les informations de la table
            if user_query.lower().startswith("select * from"):
                match = re.match(r"SELECT \* FROM ?(\w+)\.?(\w+)?", user_query, re.IGNORECASE)
                if match:
                    db_name = match.group(1).upper()
                    table_name = match.group(2).upper()
                    display_table_info(db_name, table_name)

                    # Vérification si la table existe dans la base de données
                   
                    tables = get_tables_for_db(db_name)
                    if table_name not in tables:
                         raise ValueError(f"La table '{table_name}' n'existe pas dans la base de données '{db_name}'.")


            if user_query.lower().startswith("show databases"):
    # Récupérer et afficher les bases de données
                databases = get_databases()  # Supposons que cette fonction retourne une liste des bases de données
                result_text = "Bases de Données :\n" + "\n".join(databases)
                table_info_display.config(state="normal")
                table_info_display.delete("1.0", tk.END)
                table_info_display.insert("1.0", result_text)
                table_info_display.config(state="disabled")
                return
            elif user_query.lower().startswith("show tables"):
                tables = ""
                all_databases = get_databases()
                for db in all_databases:
                    db_tables = get_tables_for_db(db)
                    tables += f"\n{db} :\n" + "\n".join([f"  - {table}" for table in db_tables]) + "\n"

                table_info_display.config(state="normal")
                table_info_display.delete("1.0", tk.END)
                table_info_display.insert("1.0", tables)
                table_info_display.config(state="disabled")
                return
            # Si une requête INSERT INTO est exécutée, afficher la table avec ses valeurs
            if user_query.lower().startswith("insert into"):
                 match = re.match(r"INSERT INTO ?(\w+)\.(\w+)", user_query, re.IGNORECASE)
                 if match:
                    db_name = match.group(1).upper()
                    table_name = match.group(2).upper()
                    display_table_info(db_name, table_name)



            
            refresh_databases()
        except Exception as e:
              # Enregistrer l'erreur dans le log d'audit

            messagebox.showerror("Erreur", f"Erreur : {str(e)}")

    refresh_databases()
    window.mainloop()

if __name__ == "__main__":
 
   setup_gui()
   
    
