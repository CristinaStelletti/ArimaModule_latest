from confluent_kafka.admin import AdminClient, NewTopic
import configparser

# Leggi il file di configurazione
config = configparser.ConfigParser()
config.read('..\config.properties')

DESTINATION_TOPIC_GIUDICE = config.get('DestinationTopics', 'DESTINATION_TOPIC_GIUDICE')
DESTINATION_TOPIC_SEZIONE = config.get('DestinationTopics', 'DESTINATION_TOPIC_SEZIONE')


# Configura il client Admin
conf = {
    'bootstrap.servers': config.get('Kafka', 'bootstrap.servers')
}

if __name__ == '__main__':

    admin_client = AdminClient(conf)

    # Definisci i nomi dei tuoi topic
    topic_names = [DESTINATION_TOPIC_GIUDICE, DESTINATION_TOPIC_SEZIONE]
    num_partitions = 1
    replication_factor = 1

    # Crea i nuovi topic
    new_topics = [NewTopic(topic, num_partitions, replication_factor) for topic in topic_names]

    # Crea i topic
    admin_client.create_topics(new_topics)

    for topic_name in topic_names:
        print(f"Il topic {topic_name} è stato creato con successo.")
