#include "StdAfx.h"
#include <stdio.h>
#include <stdlib.h>
#include <chrono>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include <pqxx/pqxx>
#include "ClientWithProducer.h" // Include the new derived class header
#include "EWrapper.h"
#include "Utils.h"
#include "bar.h"

const unsigned MAX_ATTEMPTS = 50;
const unsigned SLEEP_TIME = 10;

// Kafka producer configuration
const std::string KAFKA_BROKER = "kafka:9092";
const std::string KAFKA_TOPIC = "market_data";

// PostgreSQL connection details
const std::string PG_CONN = "dbname=yourdb user=user password=password hostaddr=127.0.0.1 port=5432";

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) override {
        std::cout << "Message delivery for (" << message.len() << " bytes): " << message.errstr() << std::endl;
    }
};

void insertHistoricalDataToTimescaleDB(const Bar& bar) {
    try {
        pqxx::connection C(PG_CONN);
        if (C.is_open()) {
            pqxx::work W(C);
            std::stringstream ss;
            ss << "INSERT INTO historical_data (time, open, high, low, close, volume, count, wap) VALUES ('"
               << bar.time << "', " << bar.open << ", " << bar.high << ", " << bar.low << ", " 
               << bar.close << ", " << bar.volume << ", " << bar.count << ", " << bar.wap << ");";
            W.exec(ss.str());
            W.commit();
            std::cout << "Inserted historical data into TimescaleDB" << std::endl;
        } else {
            std::cerr << "Can't open database" << std::endl;
        }
        C.disconnect();
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "";
    int port = argc > 2 ? atoi(argv[2]) : 0;
    if (port <= 0)
        port = 7496;
    const char* connectOptions = argc > 3 ? argv[3] : "";
    int clientId = 0;

    unsigned attempt = 0;
    printf("Start of C++ Socket Client Test %u\n", attempt);

    std::string errstr;
    ExampleDeliveryReportCb ex_dr_cb;
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", KAFKA_BROKER, errstr);
    conf->set("dr_cb", &ex_dr_cb, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return -1;
    }

    for (;;) {
        ++attempt;
        printf("Attempt %u of %u\n", attempt, MAX_ATTEMPTS);

        ClientWithProducer client(producer); // Use the derived class

        if (connectOptions) {
            client.setConnectOptions(connectOptions);
        }

        client.connect(host, port, clientId);

        while (client.isConnected()) {
            client.processMessages();
        }

        if (attempt >= MAX_ATTEMPTS) {
            break;
        }

        printf("Sleeping %u seconds before next attempt\n", SLEEP_TIME);
        std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME));
    }

    producer->flush(1000);

    delete producer;
    delete conf;

    printf("End of C++ Socket Client Test\n");
    return 0;
}
