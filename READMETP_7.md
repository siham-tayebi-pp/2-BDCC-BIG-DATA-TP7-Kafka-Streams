# TP 8: Kafka Streams
## Exercice 1 : Analyse de Données texte
### On souhaite implémenter une application Kafka Streams qui traite des messages texte envoyés
### dans un topic Kafka. - Tâches à réaliser
1. Créer les topics suivants :
   - text-input
   - text-clean
  - text-dead-letter
![img_2.png](src/images/img_2.png)
2. Lire les messages du topic text-input Chaque message est une simple chaîne de caractères.
3. Effectuer les traitements suivants :
   - Supprimer les espaces avant/après (trim)
   - Remplacer les espaces multiples par un seul espace
   - Convertir la chaîne en majuscules
```java
    //Exercice 1 : Analyse de Données texte
    public static void main(String[] args) {
     // 1. Configuration des propriétés de l'application kafka streams
        Properties props = new Properties();

        // Adresse du broker Kafka (serveur + port)
        props.put("bootstrap.servers", "localhost:9092");
        // Identifiant unique de l'application Kafka Streams
        props.put("application.id", "exercice1");
        // Configuration de la sérialisation/désérialisation des données
        // Kafka Streams manipule des données sous forme clé/valeur
        // Il est donc nécessaire de définir les types utilisés pour key et value:
        props.put("default.key.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
     // 2.Construction de la topologie de traitement des flux
        StreamsBuilder builder = new StreamsBuilder();
     // 3. Lecture/Définition du flux source à partir du topic "text-input"
        KStream<String ,String> sourceStream=builder.stream("text-input");
     // 4. Application d'une transformation sur le flux :
        // conversion des valeurs en majuscules (les clés restent inchangées)
        // Suppr les espaces via trim et rendre tt en majuscule
        KStream<String,String> streamProcessor= sourceStream.mapValues(
                value -> value
                .trim()
                .replaceAll("\\s+","")
                .toUpperCase()
        );
```
4. Filtrer les messages selon les règles suivantes :
   - Rejeter les messages vides ou constitués uniquement d’espaces
   - Rejeter les messages contenant certains mots interdits (ex. : HACK, SPAM,
   XXX)
   - Rejeter les messages dépassant une longueur de 100 caractères
```java
        List<String> motsInterdits= Arrays.asList("HACK", "SPAM","XXX");
        KStream<String,String> streamProcessorFilterValides= streamProcessor.filter(
        (k,v)-> !( v.equals("")) &&
                !(v.matches(".*X{2,}.*")) &&
                !(Arrays.stream(v.split(" ")).anyMatch(val-> (motsInterdits.contains(val)))) &&
                !(v.length()>100));
        //Messages invalides
        KStream<String,String> streamProcessorFilterInvalides= streamProcessor.filter(
                (k,v)->
                        (Arrays.stream(v.split(" ")).anyMatch(val-> (motsInterdits.contains(val)))) ||
                                (v.matches(".*X{2,}.*"))||
                                (v.length()>100)
        );
        
```
5. Routage :
   - Les messages valides (après filtrage + nettoyage) sont envoyés dans le topic text-clean
   - Les messages invalides sont envoyés tels quels dans le topic text-dead-letter
```java
// 5. Envoi du flux des messages valides  transformé vers le topic de sortie "text-clean"
        streamProcessorFilterValides.to("text-clean");
        // Envoi du flux des messages invalides  transformé vers le topic de sortie "text-clean"
        streamProcessorFilterInValides.to("text-dead-letter");

      // 6. Création de l'instance Kafka Streams avec la topologie et la configuration
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
      //  7.Démarrage de l'application Kafka Streams
        streams.start();
      // 8. Ajout d'un hook pour arrêter proprement l'application lors de l'arrêt du programme
      // L'app reste en ecoute
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
```
6. Tester :
   - Envoyer plusieurs messages (valides / invalides) dans text-input
   -  Vérifier que les messages apparaissent dans le bon topic (text-clean ou text-dead-letter)
![img_3.png](src/images/img_3.png)
![img_4.png](src/images/img_4.png)
![img_5.png](src/images/img_5.png)
![img_6.png](src/images/img_6.png)
   