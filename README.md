# GitHub-Repository-Scoring
Hajar Salame - Khaoula Arrouissi - Safaa Mahdir

Step 6 — Incremental Materialization  
-   
Pour le modèle **commits**, on choisit la stratégie "append". En effet, un commit sur GitHub est immuable. Une fois qu'il est poussé avec un sha (identifiant unique), son contenu, sa date et son auteur ne changent jamais. Il n'y a donc pas besoin de mettre à jour d'anciennes lignes (pas de merge). La stratégie "append" est la plus rapide et la moins coûteuse.  
Pour le modèle **issues**, on choisit la stratégie "merge". En effet, une issue est un objet mutable (son état change). Si une issue existante est mise à jour sur GitHub, le merge va écraser l'ancienne ligne dans ta base avec les nouvelles infos.

Questions de réflexion :  
- Modification d'un commit :  
Non, un commit est historiquement figé. Si on modifie un commit (via un rebase), son sha change, donc il est considéré comme un nouveau commit. C'est pour cela que l'append est parfait ici.
- Changement d'état d'une Issue (open → closed) :  
Contrairement au commit, une issue est mutable. Si on utilise append, on ratera la fermeture de l'issue. Pour les issues, il faut utiliser la stratégie merge avec issue_id comme unique_key. Ainsi, si l'issue existe déjà, dbt mettra à jour la colonne state et closed_at.
- Rendre la couche Gold incrémentale ?  
Problème : C'est complexe car la couche Gold contient des agrégations (ex: count(*) ou min(date)). Si on traite les données par morceaux, le calcul du "Minimum" ou du "Rang" risque d'être faux car il ne verra pas l'ensemble des données.  
  
N.B : on a implémenté la stratégie incremental juste pour les modèles commits et issues. C'est pour cela que si on exécute la commande *dbt run* après avoir ajouté les données incrémentales, les tests liés à l'unicité de certains paramètres dans repositories, scoring respositories et pull requestes ne passent plus.