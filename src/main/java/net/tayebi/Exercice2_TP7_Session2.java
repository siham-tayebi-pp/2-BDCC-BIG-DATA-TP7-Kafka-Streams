package net.tayebi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class Exercice2_TP7_Session2 {

    public static void main(String[] args) {

        // 1. Configuration Kafka Streams
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("application.id", "weather-app-final"); // change ID si besoin
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        StreamsBuilder builder = new StreamsBuilder();

        // 2. Lecture du topic
        KStream<String, String> weatherDataStream =
                builder.stream("weather-data");

        // 3. Filtrage (température >= 30°C)
        KStream<String, String> filtered =
                weatherDataStream.filter((k, v) -> {
                    try {
                        String[] parts = v.split(",");
                        return parts.length >= 3 &&
                                Double.parseDouble(parts[1]) >= 30;
                    } catch (Exception e) {
                        return false;
                    }
                });

        // 4. FIXER LA CLÉ = station ️
        KStream<String, String> keyed =
                filtered.selectKey((k, v) -> v.split(",")[0]);

        // 5. Groupement
        KGroupedStream<String, String> grouped =
                keyed.groupByKey();

        // 6. Agrégation (en Celsius)
        KTable<String, String> agg =
                grouped.aggregate(
                        () -> "0,0,0,0", // tempSum, tempCount, humSum, humCount

                        (key, value, state) -> {

                            String[] s = state.split(",");
                            String[] p = value.split(",");

                            double temp = Double.parseDouble(p[1]);
                            double hum = Double.parseDouble(p[2]);

                            double tempSum = Double.parseDouble(s[0]) + temp;
                            double tempCount = Double.parseDouble(s[1]) + 1;

                            double humSum = Double.parseDouble(s[2]) + hum;
                            double humCount = Double.parseDouble(s[3]) + 1;

                            return tempSum + "," + tempCount + "," + humSum + "," + humCount;
                        },

                        Materialized.with(Serdes.String(), Serdes.String())
                );

        // 7. Calcul des moyennes + conversion finale
        KStream<String, String> result =
                agg.toStream().mapValues((key, state) -> {

                    String[] s = state.split(",");

                    double tempMoyC = Double.parseDouble(s[0]) / Double.parseDouble(s[1]);
                    double humMoy = Double.parseDouble(s[2]) / Double.parseDouble(s[3]);

                    // Conversion en Fahrenheit à la FIN
                    double tempMoyF = tempMoyC * 9.0 / 5.0 + 32.0;

                    return key + " : Température Moyenne = " + tempMoyF +
                            "°F, Humidité Moyenne = " + humMoy + "%";
                });

        // DEBUG
        result.peek((k, v) -> System.out.println("OUTPUT => " + k + " : " + v));

        // 8. Envoi vers topic final
        result.to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // 9. Lancement
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}