# test-candidat-Weenat

Lancement :
Bien s'assurer que le serveur node contenant les données soit lancé en localhost:3000

Packages :
<ol>
    <li>Plotly</li>
    <li>Pandas</li>
    <li>Pytest</li>
    <li>Numpy</li>
    <li>fastapi</li>
    <li>requests</li>
    <li>httpx</li>
    <li>orjson</li>
</ol>


Bien les installer avec la commande :
```pip install plotly, pandas, pytest, numpy, fastapi, requests, httpx, orjson```

lancer l'application avec ```python main.py```
puis se connecter à  [http://localhost:8000/](http://localhost:8000/)


les données vont jusqu'à fin 2022 donc ça ne sert à rien de faire des requêtes en 2023 uniquement ^^

Au moment de demander un aggrégat, le schéma généré ne s'affiche pas en html, vous le retrouverez dans app/resources/static/plot.html

Tout les commentaires en français dans le code indiquent des features imaginées mais non implémentées, ou juste des bugs
