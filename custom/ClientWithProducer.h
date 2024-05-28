#ifndef CLIENTWITHPRODUCER_H
#define CLIENTWITHPRODUCER_H

#include "TestCppClient.h"
#include <librdkafka/rdkafkacpp.h>
#include "Utils.h"  // Include the Utils header

extern const std::string KAFKA_TOPIC;  // Forward declare KAFKA_TOPIC
void insertHistoricalDataToTimescaleDB(const Bar& bar);  // Forward declare the function

class ClientWithProducer : public TestCppClient {
public:
    ClientWithProducer(RdKafka::Producer* producer)
        : TestCppClient(), producer(producer) {}

    void historicalData(TickerId reqId, const Bar& bar) override {
        std::cout << "HistoricalData. " << reqId << " - Date: " << bar.time << ", Open: " << Utils::doubleMaxString(bar.open)
                  << ", High: " << Utils::doubleMaxString(bar.high) << ", Low: " << Utils::doubleMaxString(bar.low)
                  << ", Close: " << Utils::doubleMaxString(bar.close) << ", Volume: " << decimalStringToDisplay(bar.volume)
                  << ", Count: " << Utils::intMaxString(bar.count) << ", WAP: " << decimalStringToDisplay(bar.wap) << std::endl;

        insertHistoricalDataToTimescaleDB(bar);

        if (producer) {
            std::stringstream ss;
            ss << "{\"time\":\"" << bar.time << "\",\"open\":" << bar.open << ",\"high\":" << bar.high << ",\"low\":"
               << bar.low << ",\"close\":" << bar.close << ",\"volume\":" << bar.volume << ",\"count\":" << bar.count
               << ",\"wap\":" << bar.wap << "}";
            std::string data = ss.str();
            RdKafka::ErrorCode resp = producer->produce(KAFKA_TOPIC, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, 
                const_cast<void*>(static_cast<const void*>(data.c_str())), data.size(), nullptr, 0, 0, nullptr);

            if (resp != RdKafka::ERR_NO_ERROR) {
                std::cerr << "Failed to send message: " << RdKafka::err2str(resp) << std::endl;
            }
            producer->poll(0);
        }
    }

private:
    RdKafka::Producer* producer;
};

#endif // CLIENTWITHPRODUCER_H
