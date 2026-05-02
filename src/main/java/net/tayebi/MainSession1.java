package net.tayebi;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MainSession1 {
    public static void main(String[] args) throws StreamsException {
        System.out.println("Hello, World!");
        //Config lapp kafaka stream
        //kafa stream lit les msg dans un broker kafka donc il faut lui donner un port
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");//le nom de lapp
        props.put("bootstrap.servers", "localhost:9092");
//        kafka lit donnees ss forme de cle val
        //la les donnes sont serialise c pour cela on doit ecrire / speciifer preciser les types
        props.put("default.key.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        //Construire le flux
        //on doit ecrir un src processor pour genere un kstram
        StreamsBuilder builder = new StreamsBuilder();
        //lit donnee dpuis src et renvoie un stream
        KStream<String,String> sourceStream=builder.stream("input-topic");
        //converir que les val en majuscule
        KStream<String,String> upperCaseStream =sourceStream.mapValues((str)->str.toUpperCase());
        //envoie res vers output topic
        upperCaseStream.to("output-topic");
        KafkaStreams streams=new KafkaStreams(builder.build(),props);
        streams.start();

        //Apres 3aad anzido donnees , strings
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        //
    }
}