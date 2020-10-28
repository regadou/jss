
                  JavaScript Servlet (JSS)
                  
JSS est le servlet utilisé pour servir les pages dynamiques du site de web de l'Association de Généalogie d'Haïti. Au départ, l'objectif était de porter en Java un interpréteur javascript maison écrit en C qui fonctionnait en CGI. Ce servlet permet maintenant de programmer en Javascript sur n'importe quel serveur J2EE et permet l'accès à toutes les classes Java à partir du script. Cet interpréteur peut aussi être utilisé pour l'éxécution de scripts en mode console et en local.

- Pour modifier le code, voir dans le répertoire src
- Pour compiler et packager, vous devez avoir Maven
  - la commande à éxécuter: mvn package
  - le nouveau fichier jss.jar sera dans ../html/WEB-INF/lib
- Pour ajouter des jars de dépendance, il faut les mettre dans ../html/WEB-INF/lib

