#include "StdAfx.h"

#include <stdio.h>
#include <stdlib.h>
#include <chrono>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include <pqxx/pqxx>
#include "TestCppClient.h"

const unsigned MAX_ATTEMPTS = 50;
const unsigned SLEEP_TIME = 10;

// Kafka producer configuration
const std::string KAFKA_BROKER = "localhost:9092";
const std::string KAFKA_TOPIC = "historical_data";

// PostgreSQL connection details
const std::string PG_CONN = "dbname=yourdb user=user password=password hostaddr=127.0.0.1 port=5432";

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) override {
        std::cout << "Message delivery for (" << message.len() << " bytes): " << message.errstr() << std::endl;
    }
};

// Function to insert historical data into TimescaleDB
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

class TestCppClient : public EWrapper {
    // Declaring the required methods that will be overridden
public:
    TestCppClient(RdKafka::Producer* prod) : producer(prod) {}

    // Override historicalData function to insert data into TimescaleDB and send to Kafka
    void historicalData(TickerId reqId, const Bar& bar) override;

    // Other existing methods...
    bool connect(const char* host, int port, int clientId);
    bool isConnected() const;
    void processMessages();

private:
    RdKafka::Producer* producer;
};

// Implementation of the overridden historicalData function
void TestCppClient::historicalData(TickerId reqId, const Bar& bar) {
    printf("HistoricalData. ReqId: %ld - Date: %s, Open: %s, High: %s, Low: %s, Close: %s, Volume: %s, Count: %s, WAP: %s\n", 
           reqId, bar.time.c_str(), Utils::doubleMaxString(bar.open).c_str(), Utils::doubleMaxString(bar.high).c_str(), 
           Utils::doubleMaxString(bar.low).c_str(), Utils::doubleMaxString(bar.close).c_str(), decimalStringToDisplay(bar.volume).c_str(), 
           Utils::intMaxString(bar.count).c_str(), decimalStringToDisplay(bar.wap).c_str());

    // Insert data into TimescaleDB
    insertHistoricalDataToTimescaleDB(bar);

    // Prepare data for Kafka
    std::stringstream ss;
    ss << "{\"time\":\"" << bar.time << "\",\"open\":" << bar.open << ",\"high\":" << bar.high << ",\"low\":"
       << bar.low << ",\"close\":" << bar.close << ",\"volume\":" << bar.volume << ",\"count\":" << bar.count
       << ",\"wap\":" << bar.wap << "}";

    std::string data = ss.str();
    RdKafka::ErrorCode resp = producer->produce(KAFKA_TOPIC, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, 
                                                const_cast<char *>(data.c_str()), data.size(), nullptr, nullptr);

    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to send message: " << RdKafka::err2str(resp) << std::endl;
    }

    producer->poll(0);
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

    // Kafka setup
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

        TestCppClient client(producer);

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
