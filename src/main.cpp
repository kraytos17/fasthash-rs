// Copyright 2025 Soubhik Gon
#include <iostream>
#include <print>
#include <string>

#include "core/command.hpp"
#include "core/fast-hash.hpp"

int main() {
    fasthash::FastHash store("appendonly.aof");
    // store.replayAOF("appendonly.aof");

    std::string line;
    std::print("FastHash CLI. Commands: SET, GET, DEL, EXPIRE, TTL, SETEX\n");
    while (true) {
        std::print("> ");
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line.empty()) {
            continue;
        }

        fasthash::Command cmd = fasthash::Command::parse(line);
        if (cmd.type == fasthash::Command::Type::INVALID) {
            std::print("ERROR: Invalid command\n");
            continue;
        }
        cmd.execute(store);
    }
    store.stop();

    return 0;
}
