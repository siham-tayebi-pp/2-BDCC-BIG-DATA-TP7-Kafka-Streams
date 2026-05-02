package net.tayebi;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
//Stateless processing
public class KafkaStreamsSession2 {
    public static void main(String[] args) throws StreamsException {
        // Config des props de lapp kafka streams
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");//adress borker kafka
        props.put("application.id", "KafkaStreamsSession2-Application");// id app
        //specification des types des donnees qui seront serialiser
        props.put("default.key.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde","org.apache.kafka.common.serialization.Serdes$StringSerde");

        //Construire topologie de traitemnt de flux e construire le flux
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String ,String> ordersStream=builder.stream("orders");
        //oreders amount>100
        KStream<String ,String> filteredOrders=ordersStream.filter((k,v)-> {
            double amount=Double.parseDouble(v.split(",")[1]);
            return amount > 100;// retrn que order avec amount 100
        });
        // ajouter 10 % de taxe sur chauqe amount
        KStream<String,String> ordersWithTaxes=filteredOrders.mapValues(v->{
            double amount=Double.parseDouble(v.split(",")[1]);
            return v.split(",")[0]+"-"+ (amount+0.1*amount);
        });
        //Stateless
        //grouper les cmds par nom de client (nom client, amount)
        KGroupedStream<String,String> groupedOrders=ordersWithTaxes.groupBy((k,v)->v.split(",")[0]);
        //resulta de groupemnt staocke dans k table : cle c client et val c total
        // 0.0 c val def de somme
        KTable<String,Double> totalByCustomer=groupedOrders.aggregate(()-> 0.0,(key,val,somme) -> {
                    Double amount=Double.parseDouble(val.split("-")[1]);
                   return  somme + amount;
          },
                Materialized.with(Serdes.String(),Serdes.Double())

        );
        totalByCustomer.toStream().mapValues(v->String.valueOf(v)).to("costumer-total",Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        //arret propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));






    }
}