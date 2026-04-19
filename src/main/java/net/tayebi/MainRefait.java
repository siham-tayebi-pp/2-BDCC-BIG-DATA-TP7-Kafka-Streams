package net.tayebi;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MainRefait {
    public static void main(String[] args) {
        //Declaration de lobj properties
        Properties props = new Properties();
        //saissir ladresse et le port de kafka
        props.put("bootstrap.servers", "localhost:9092");
        //preciser lid de lapp
        props.put("application.id","Kafka-first-application");
        //saisir les types de  notre steam qui est ss forme de cle valeur :
        // cad specifier type de cle et d val car ca devra etre serialise
        //les types qui sont serialise
        props.put("default.key.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        //creer la topologie via StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();
        // flux de cle val strings
        KStream<String ,String> sourceStream=builder.stream("input-topic");
        // appliquer un streamProcessor sur un stream
        // mapvalues je veux appliquer que sur val cle ca va rester comme elle
        KStream<String ,String> upperCaseProcessorStream =sourceStream.mapValues((str)->str.toUpperCase());
        // envoyer le resultat/mom stream  vers un autre topic
        upperCaseProcessorStream.to("output-topic");
        // StreamBuilder lui qui permet de contruire la topologie de lapp via src stream et sink

        //demarrer kafka streams et lui donner la topologie a execute  et les proprietes de broker kafka
        KafkaStreams  streams=new KafkaStreams(builder.build(), props);
        streams.start();
        //pour que lapp reste ne ecout de linput topic
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
